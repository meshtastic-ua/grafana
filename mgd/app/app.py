import random
from meshtastic import mesh_pb2 as mesh_pb2
from meshtastic import mqtt_pb2 as mqtt_pb2
from meshtastic import telemetry_pb2 as telemetry_pb2
from meshtastic import portnums_pb2 as PortNum

#from paho.mqtt import client as mqtt_client
import paho.mqtt.client as mqtt

from collections import deque

from peewee import *
import datetime
import base64

import configparser

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.rest import ApiException

from google.protobuf.json_format import MessageToDict

message_deque = deque(maxlen=60)

config = configparser.RawConfigParser()
config.read('config.ini')

pg_db = PostgresqlDatabase(config['Postgres']['database'], user=config['Postgres']['user'], 
    password=config['Postgres']['password'],host=config['Postgres']['host'], port=5432)

cursor = pg_db.cursor()


class BaseModel(Model):
    class Meta:
        database = pg_db  

class Chat(BaseModel):
    chat_id = AutoField(column_name='id', primary_key = True)
    user_id = BigIntegerField(column_name='did')
    text = CharField(column_name='text', null=True)
    time = DateTimeField(column_name='dt', default=datetime.datetime.now(datetime.timezone.utc))
    gateway_id = BigIntegerField(column_name='gateway_id')

    class Meta:
        table_name = 'chat'

class Users(BaseModel):
    user_id = BigIntegerField(column_name='did', primary_key = True, unique=True)
    long_name = CharField(column_name='long_name')
    short_name = CharField(column_name='short_name', null=True)
    hw_model = CharField(column_name='hw_model', null=True)
    last_dt = DateTimeField(column_name='last_dt', default=datetime.datetime.now(datetime.timezone.utc))
    rssi = IntegerField(column_name='rssi', null=True)
    rxsnr = FloatField(column_name='rxsnr', null=True)
    hoplimir = IntegerField(column_name='hoplimir', null=True)
    lat = DoubleField(column_name='lat', null=True)
    longt = DoubleField(column_name='long', null=True)
    alt = IntegerField(column_name='alt', null=True)
    batlevel = IntegerField(column_name='batlevel', null=True)
    voltage = DoubleField(column_name='voltage', null=True)
    chutil = DoubleField(column_name='chutil', null=True)
    airuntiltx = DoubleField(column_name='airuntiltx', null=True)
    envtemp = DoubleField(column_name='envtemp', null=True)
    envrelhum = DoubleField(column_name='envrelhum', null=True)
    envbarpress = DoubleField(column_name='envbarpress', null=True)
    envgasres = DoubleField(column_name='envgasres', null=True)
    envvoltage = DoubleField(column_name='envvoltage', null=True)
    envcurr = DoubleField(column_name='envcurr', null=True)
    gateway_id = BigIntegerField(column_name='gateway_id')

    class Meta:
        table_name = 'users'


def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    #client.subscribe("$SYS/#")
    client.subscribe(config['MQTT']['topic'])

# def connect_mqtt() -> mqtt_client:
#     def on_connect(client, userdata, flags, rc):
#         if rc == 0:
#             print("Connected to MQTT Broker!")
#         else:
#             print("Failed to connect, return code %d\n", rc)

#     client = mqtt_client.Client(client_id)
#     client.username_pw_set(username, password)
#     client.on_connect = on_connect
#     client.connect(broker, port)
#     return client


def write_influx(data):
    try:
        bucket = f'{config["InfluxDB"]["database"]}/{config["InfluxDB"]["retention_policy"]}'
        with InfluxDBClient(url=config['InfluxDB']['url'], 
            token=f'{config["InfluxDB"]["user"]}:{config["InfluxDB"]["password"]}', org='-')  as client:
            with client.write_api() as write_api:
                point = Point.from_dict(data)
                u = write_api.write(bucket=bucket, record=point, write_options=SYNCHRONOUS)
                #print(u)
    except ApiException as e:
        # bucket does not exist
        if e.status == 404:
            raise Exception(f"The specified bucket does not exist.") from e
        # insufficient permissions
        if e.status == 403:
            raise Exception(f"The specified token does not have sufficient credentials to write to '{bucket}'.") from e
        # 400 (BadRequest) caused by empty LineProtocol
        if e.status != 400:
            raise

# prev = 0
#def subscribe(client: mqtt_client):
def on_message(client, userdata, msg):
    #print(f"Received `{msg.payload}` from `{msg.topic}` topic")
    m = mqtt_pb2.ServiceEnvelope().FromString(msg.payload)
    # global prev
    # if(m.packet.id != prev):
    #     print('----')
    #     prev = m.packet.id
    # print(m.packet.id, m.packet.hop_limit, m.gateway_id)
    if(m.packet.id not in message_deque):
        message_deque.append(m.packet.id)
        #print(m)
        did = m.packet.fram
        gateway_id = int(m.gateway_id[1:], 16)
        #print(gateway_id, did)
        #print(m)
        try:
            ooo = MessageToDict(m.packet)

            if 'decoded' in ooo.keys():
                if 'portnum' in ooo['decoded'].keys():
                    ooo['portnum'] = ooo['decoded']['portnum']
                if 'payload' in ooo['decoded'].keys():
                    ooo['payload'] = ooo['decoded']['payload']
                ooo.pop("decoded")

            if 'encrypted' in ooo.keys():
                ooo['portnum'] = 'encrypted'
                ooo['encrypted'] = str(base64.b64encode(str.encode(str(ooo['encrypted']))))

            ooo['gateway_id'] = gateway_id

            #print(ooo)
            dict_structure = {
                "measurement": "raw",
                "tags": {"idName": did, "gateway_id": gateway_id},
                "fields": ooo
            }
            write_influx(dict_structure)

            dec = m.packet.decoded
            #print(dec)
            payload = dec.payload
            # if type(payload) == unicode:
            #     data = bytearray(payload, "utf-8")
            #     payload = bytes(data)
            #print(type(dec.portnum))
            #print(dir(PortNum))
            if dec.portnum == PortNum.NODEINFO_APP:  
                p = mesh_pb2.User().FromString(payload)

                obg_keys = [Users.long_name, Users.short_name, Users.hw_model, Users.rssi, Users.rxsnr, Users.hoplimir]
                keys = ["long_name", "short_name", "hw_model", "rx_rssi", "rx_snr", "hop_limit"]
                keys_d = ["rx_rssi", "rx_snr", "hop_limit"]
                values = {}
                #print(m.packet)
                for metr in keys:
                    #print("1" , values)
                    try:
                        if getattr(p, metr):
                            values[metr] = getattr(p, metr, None)
                        else:
                            values[metr] = None
                    except:
                        pass
                    try:
                        if getattr(m.packet, metr) != 0.0:
                            values[metr] = getattr(m.packet, metr, None)
                        else:
                            values[metr] = None
                    except:
                        pass

                #print("2", values)

                d_fields = {}
                for metr in keys_d:
                    if values[metr] is not None:
                        d_fields[metr] = values[metr] 
                        d_fields["gateway_id"] = gateway_id
                    if metr in "hw_model":
                        d_fields[metr] = mesh_pb2.HardwareModel.Name(values[metr])
                        d_fields["gateway_id"] = gateway_id

                #print("3", d_fields)

                q_fields = {}
                for v1, v2 in zip(obg_keys, keys):
                    if values[v2] is not None:
                        q_fields[v1] = values[v2]
                        q_fields[Users.gateway_id] = gateway_id
                    if v2 in "hw_model":
                        q_fields[v1] = mesh_pb2.HardwareModel.Name(values[v2])
                        q_fields[Users.gateway_id] = gateway_id

                q_fields[Users.last_dt] = datetime.datetime.now(datetime.timezone.utc)
                #print("4", q_fields)

                if q_fields:
                    query = (Users.insert(
                        user_id=did, long_name=p.long_name, short_name=p.short_name, hw_model=mesh_pb2.HardwareModel.Name(p.hw_model),
                        rssi=m.packet.rx_rssi, rxsnr=m.packet.rx_snr, hoplimir=m.packet.hop_limit)
                        .on_conflict(conflict_target=[Users.user_id],
                            update=q_fields)
                        )

                    #print(query)
                    query.execute()

                if d_fields:
                    dict_structure = {
                        "measurement": "info",
                        "tags": {"idName": did, "gateway_id": gateway_id},
                        "fields": d_fields
                    }
                    write_influx(dict_structure)

            elif dec.portnum == PortNum.TELEMETRY_APP:
                #print('test1')
                p = telemetry_pb2.Telemetry().FromString(payload)

                if 'device_metrics' in dir(p):
                    obg_keys = [Users.batlevel, Users.voltage, Users.chutil, Users.airuntiltx, Users.last_dt]
                    keys = ["battery_level", "voltage", "channel_utilization", "air_util_tx"]
                    values = {}
                    k = p.device_metrics

                    for metr in keys:
                        if getattr(k, metr) != 0.0:
                            values[metr] = getattr(k, metr, None)
                        else:
                            values[metr] = None

                    #print(values)

                    d_fields = {}
                    for metr in keys:
                        if values[metr] is not None:
                            d_fields[metr] = values[metr]
                            d_fields["gateway_id"] = gateway_id


                    if d_fields:
                        q_fields = {}
                        for v1, v2 in zip(obg_keys, keys):
                            if values[v2] is not None:
                                q_fields[v1] = values[v2]
                                q_fields[Users.gateway_id] = gateway_id

                        q_fields[Users.last_dt] = datetime.datetime.now(datetime.timezone.utc)
                        #print(q_fields)

                        query = (Users.insert(
                            user_id = did, batlevel=values["battery_level"], voltage=values["voltage"], chutil=values["channel_utilization"], airuntiltx=values["air_util_tx"])
                            .on_conflict(conflict_target=[Users.user_id],
                                update=q_fields)
                            )

                        #print(query)
                        query.execute()

                        dict_structure = {
                            "measurement": "info",
                            "tags": {"idName": did, "gateway_id": gateway_id},
                            "fields": d_fields
                        }
                        write_influx(dict_structure)

                if 'environment_metrics' in dir(p):
                    obg_keys = [Users.envtemp, Users.envrelhum, Users.envbarpress, Users.envgasres, Users.envvoltage, Users.envcurr]
                    keys = ["temperature", "relative_humidity", "barometric_pressure", "gas_resistance", "env_voltage", "current"]
                    values = {}
                    k = p.environment_metrics
                    for metr in keys:
                        if getattr(k, metr) != 0.0:
                            values[metr] = getattr(k, metr, None)
                        else:
                            values[metr] = None

                    #print(values)

                    d_fields = {}
                    for metr in keys:
                        if values[metr] is not None:
                            d_fields[metr] = values[metr]
                            d_fields["gateway_id"] = gateway_id

                    if d_fields:
                        dict_structure = {
                            "measurement": "info",
                            "tags": {"idName": did, "gateway_id": gateway_id},
                            "fields": d_fields
                        }
                        #print(d_fields)
                        write_influx(dict_structure)

                        q_fields = {}
                        for v1, v2 in zip(obg_keys, keys):
                            if values[v2] is not None:
                                q_fields[v1] = values[v2]
                                q_fields[Users.gateway_id] = gateway_id

                        q_fields[Users.last_dt] = datetime.datetime.now(datetime.timezone.utc)
                        #print(d_fields, q_fields)

                        query = (Users.insert(
                            user_id = did, envtemp=values["temperature"], envrelhum=values["relative_humidity"], envbarpress=values["barometric_pressure"]
                            , envgasres=values["gas_resistance"], envvoltage=values["env_voltage"], envcurr=values["current"])
                            .on_conflict(conflict_target=[Users.user_id],
                                update=q_fields)
                            )

                        #print(query)
                        query.execute()
                    # dict_structure = {
                    #     "measurement": "info",
                    #     "tags": {"idName": did},
                    #     "fields": {
                    #         "envCurr": float(k.current),
                    #         "envVolt": float(k.voltage),
                    #         "gasRes": float(k.gas_resistance),
                    #         "barPress": float(k.barometric_pressure),
                    #         "relHum": float(k.relative_humidity),
                    #         "temp": float(k.temperature)
                    #     }
                    # }



            elif dec.portnum == PortNum.POSITION_APP:
                p = mesh_pb2.Position().FromString(payload)

                obg_keys = [Users.lat, Users.longt, Users.alt]
                keys = ["latitude_i", "longitude_i", "altitude"]
                val_mod = {"latitude_i":0.0000001, "longitude_i":0.0000001, "altitude":1.0}
                values = {}

                for metr in keys:
                    if getattr(p, metr) != 0.0:
                        values[metr] = getattr(p, metr, None)
                    else:
                        values[metr] = None

                #print(values)

                d_fields = {}
                for metr in keys:
                    if values[metr] is not None:
                        d_fields[metr] = values[metr] * val_mod[metr]
                        d_fields["gateway_id"] = gateway_id


                if d_fields:
                    q_fields = {}
                    for v1, v2 in zip(obg_keys, keys):
                        if values[v2] is not None:
                            q_fields[v1] = values[v2] * val_mod[v2]
                            q_fields[Users.gateway_id] = gateway_id

                    q_fields[Users.last_dt] = datetime.datetime.now(datetime.timezone.utc)
                    #print(q_fields)

                    query = (Users.insert(
                        user_id = did, lat=p.latitude_i * 0.0000001, longt=p.longitude_i * 0.0000001, alt=p.altitude)
                        .on_conflict(conflict_target=[Users.user_id],
                            update=q_fields)
                        )

                    #print(query)
                    query.execute()

                    dict_structure = {
                        "measurement": "pos",
                        "tags": {"idName": did, "gateway_id": gateway_id},
                        "fields": d_fields
                    }
                    #print(dict_structure)
                    write_influx(dict_structure)

            elif dec.portnum == PortNum.TEXT_MESSAGE_APP:
                #p = bytearray(payload, "utf-8")
                p = payload
                #print(p)
                #hat_msg = Chat(user_id=m.packet.fram, text = p, dt = datetime.datetime.now(datetime.timezone.utc))
                #chat_msg.save()
                chat = Chat.create(user_id=m.packet.fram, text = p, gateway_id = gateway_id, time = datetime.datetime.now(datetime.timezone.utc))
                #print(chat, datetime.datetime.now(datetime.timezone.utc))
                #print(chat_msg.chat_id)

            #print(p)


        except Exception as e: print(e)


    # client.subscribe(topic)
    # client.on_message = on_message


def run():
    # cursor.execute("SELECT * FROM Chat ORDER BY dt LIMIT 3")
    # results = cursor.fetchall()
    # print(results)


    # query = Chat.select().execute()
    # print(query)

    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    client.username_pw_set(config['MQTT']['username'], config['MQTT']['password'])

    # client = connect_mqtt()
    # subscribe(client)
    # client.loop_forever()

    while True:
        try:
            client.connect(config['MQTT']['broker'], int(config['MQTT']['port']), 60)
        except socket.timeout:
            print('Connect timeout...')
            time.sleep(10)
        # Blocking call that processes network traffic, dispatches callbacks and
        # handles reconnecting.
        # Other loop*() functions are available that give a threaded interface and a
        # manual interface.
        try:
            client.loop_forever()
        except TimeoutError:
            print('Loop timeout...')
            time.sleep(10)

if __name__ == '__main__':
    try:
        run()
    except Exception as e: 
        print(e)
        print('Close DB')
        pg_db.close()
