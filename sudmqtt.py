#!/usr/bin/env python
#
# Read Seneye SUD and publish readings to MQTT
# MQTT is published using the 'single' publish instead of opening a client connection
# See the file protocol.mdown for a description of the SUD communications flow
#
import json
import pprint
import sys
import argparse
import time

import usb.core
import usb.util
from bitstring import BitArray
from awscrt import io, mqtt, auth, http
from awsiot import mqtt_connection_builder

import datetime

interface = 0
topic = "raw/aquarium"

vendor = 9463
product = 8708

timestamp = datetime.datetime.utcnow().isoformat()

parser = argparse.ArgumentParser(description="Read and send Seneye readings to AWS IOT")

parser.add_argument('--endpoint', required=True, help="Your AWS IoT custom endpoint, not including a port. " +
                                                    "Ex: \"abcd123456wxyz-ats.iot.us-east-1.amazonaws.com\"")
parser.add_argument('--cert', help="File path to your client certificate, in PEM format.")
parser.add_argument('--key', help="File path to your private key, in PEM format.")
parser.add_argument('--root-ca', help="File path to root certificate authority, in PEM format. " +
                                    "Necessary if MQTT server uses a certificate that's not already in " +
                                    "your trust store.")

parser.add_argument('--client-id', default='samples-client-id', help="Client ID for MQTT connection.")
parser.add_argument('--topic', default="samples/test", help="Topic to subscribe to, and publish messages to.")

args = parser.parse_args()

def printhex(s):
    return(type(s),len(s),":".join("{:02x}".format(c) for c in s))

def printbit(s): 
    return(type(s),len(s),":".join("{:02x}".format(c) for c in s))

def set_up():
    # find the device using product id strings
    dev = usb.core.find(idVendor=vendor, idProduct=product)
    if __debug__:
        print("device       >>>",dev)
    return(dev)

def read_sud(dev, interface):
    # release kernel driver if active
    if dev.is_kernel_driver_active(interface):
        dev.detach_kernel_driver(interface)

    # by passing no parameter we pick up the first configuration, then claim interface, in that order
    dev.set_configuration()
    usb.util.claim_interface(dev, interface)
    configuration = dev.get_active_configuration()
    interface = configuration[(0,0)]
    if __debug__:
        print("configuration>>>",configuration)
        print("interface    >>>",interface)

    # find the first in and out endpoints in our interface
    epIn = usb.util.find_descriptor(interface, custom_match= lambda e: usb.util.endpoint_direction(e.bEndpointAddress) == usb.util.ENDPOINT_IN)
    epOut = usb.util.find_descriptor(interface, custom_match = lambda e: usb.util.endpoint_direction(e.bEndpointAddress) == usb.util.ENDPOINT_OUT)

    # were our endpoints found?
    assert epIn is not None
    assert epOut is not None
    if __debug__:
        print("endpoint in  >>>",epIn)
        print("endpoint out >>>",epOut)

    # write to device with hello string
    msg="HELLOSUD"
    rc=dev.write(epOut,msg)
    if __debug__:
        print("HELO ret code>>>",rc)

    # read from device
    ret=dev.read(epIn,epIn.wMaxPacketSize)
    if __debug__:
        print("HELO hex     >>>",printhex(ret))

    # write to device with reading request
    msg="READING"
    rc=dev.write(epOut,msg)
    if __debug__:
        print("READ ret code>>>",rc)

    # read from device twice, first for return from "READING", second for actual values
    ret=dev.read(epIn,epIn.wMaxPacketSize,1000)
    ret=dev.read(epIn,epIn.wMaxPacketSize,10000)
    c = BitArray(ret)
    if __debug__:
        print("sensor hex   >>>",printhex(ret))
        print("sensor bits len>",len(c.bin))
        print("sensor bits  >>>",c.bin)
        
    # write to device with close string
    msg="BYESUD"
    rc=dev.write(epOut,msg)
    if __debug__:
        print("BYE ret code >>>",rc)
    return(c)

def mungReadings(p):
    # see protocol.mdown for explaination of where the bitstrings start and end
    s={}
    i=37
    s['InWater']=p[i]
    s['SlideNotFitted']=p[i+1]
    s['SlideExpired']=p[i+2]
    ph=p[80:96]
    s['pH']=ph.uintle/100.00   # divided by 100
    nh3=p[96:112]
    s['NH3']=nh3.uintle/1000.00  # divided by 1000
    temp=p[112:144]
    s['Temp']=temp.intle/1000.00 # divided by 1000
    s['timestamp'] = timestamp
    if __debug__:
        pprint.pprint(s)
    j = json.dumps(s, ensure_ascii=False)
    return(j)

def clean_up(dev):
    # re-attach kernel driver
    usb.util.release_interface(dev, interface)
    dev.attach_kernel_driver(interface)
    # clean up
    usb.util.release_interface(dev, interface)
    usb.util.dispose_resources(dev)
    dev.reset()

# Callback when connection is accidentally lost.
def on_connection_interrupted(connection, error, **kwargs):
    print("Connection interrupted. error: {}".format(error))


# Callback when an interrupted connection is re-established.
def on_connection_resumed(connection, return_code, session_present, **kwargs):
    print("Connection resumed. return_code: {} session_present: {}".format(return_code, session_present))

    if return_code == mqtt.ConnectReturnCode.ACCEPTED and not session_present:
        print("Session did not persist. Resubscribing to existing topics...")
        resubscribe_future, _ = connection.resubscribe_existing_topics()

        # Cannot synchronously wait for resubscribe result because we're on the connection's event-loop thread,
        # evaluate result with a callback instead.
        resubscribe_future.add_done_callback(on_resubscribe_complete)

def on_resubscribe_complete(resubscribe_future):
        resubscribe_results = resubscribe_future.result()
        print("Resubscribe results: {}".format(resubscribe_results))

        for topic, qos in resubscribe_results['topics']:
            if qos is None:
                sys.exit("Server rejected resubscribe to topic: {}".format(topic))

def main(args):
    # open device
    device = set_up()
    # read device
    sensor = read_sud(device, interface)
    # format into json
    readings = mungReadings(sensor)
    # push readings to MQTT broker
    event_loop_group = io.EventLoopGroup(1)
    host_resolver = io.DefaultHostResolver(event_loop_group)
    client_bootstrap = io.ClientBootstrap(event_loop_group, host_resolver)

    mqtt_connection = mqtt_connection_builder.mtls_from_path(
        endpoint=args.endpoint,
        cert_filepath=args.cert,
        pri_key_filepath=args.key,
        client_bootstrap=client_bootstrap,
        ca_filepath=args.root_ca,
        on_connection_interrupted=on_connection_interrupted,
        on_connection_resumed=on_connection_resumed,
        client_id=args.client_id,
        clean_session=False,
        keep_alive_secs=6)
    
    connect_future = mqtt_connection.connect()
 
    message = "{} [{}]".format(readings, 1)
    response = mqtt_connection.publish(
        topic=args.topic,
        payload=readings,
        qos=mqtt.QoS.AT_LEAST_ONCE)
   
    time.sleep(1)
    print("Disconnecting...")
    disconnect_future = mqtt_connection.disconnect()
    disconnect_future.result()
    print("Disconnected!")

    # close device
    clean_up(device)

if __name__ == "__main__":
    main(args)
