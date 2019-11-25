#para realizar os testes:
#inscreva-se no topico "a": $ mosquitto_sub -t "a"
#envie os dados no topico "a" através de: $ mosquitto_pub -t "a" -


import threading
import paho.mqtt.client as paho
import paho.mqtt.subscribe as subscribe
import paho.mqtt.publish as publish
import json

broker = "localhost"


def on_connect(client, userdata, flags, rc):
    client.subscribe("#")


def on_message(client, userdata, msg):
    # read_json()
    compose(msg)


def read_json():
    #:TODO fazer loop para ler todos os dados do json (e não só o exec1)
    with open('config.json') as file_json:
        data = json.load(file_json)
        # print(data)
    string = (
        data['exec1']['func'],
        data['exec1']['input'],
        data['exec1']['size'],
        data['exec1']['output']
    )
    # for d in data:
    #         string=(
    #                 data[d]['func'],
    #                 data[d]['input'],
    #                 data[d]['size'],
    #                 data[d]['output']
    #                 )
    # print(string)
    return string


def compose(a):  # orquestra qual função será aplicada --> Scheduling:
    string = read_json()
    print(string)
    # verifica qual é a função que o json solicita e direciona os dados pra ela
    if(string[0] == "mean"):
        # foo = threading.Thread(target=mean_topic,args=(string))
        mean_topic(string)
    if(string[0] == "sum"):
        sum_topic(string)

def mean_topic(message):
    # TODO: mudar para subscribe
    buffer = []
    saida = 0
    print(len(buffer))
    while len(buffer) < int(message[2]):
        print("entrei")
        print("tamannho do buffer:", buffer)
        m = subscribe.simple(message[1])  # subscreve para receber 1 mensagem por vez
        data = m.payload.decode("utf-8")
        buffer.append(int(data))
    saida = sum(buffer)/len(buffer)
    publish.single(message[3], saida)


def sum_topic(message):
    buffer = []
    saida = 0
    while len(buffer) < int(message[2]):
        m = subscribe.simple(message[1])
        data = m.payload.decode("utf-8")
        buffer.append(int(data))
    saida = sum(buffer)
    publish.single(message[3], saida)


client = paho.Client("foo")
client.on_connect = on_connect
client.on_message = on_message

client.connect(broker)
client.loop_forever()

