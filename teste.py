import threading
import paho.mqtt.client as paho
import paho.mqtt.subscribe as subscribe
import paho.mqtt.publish as publish
import json

import time

broker = "localhost"

clients = [] 
# lista de clientes
# [
#   {"client":"objetoClient", "sub_topic":"topico-subscribe","pub_topic":"topico-publish","func":"funcao", "size":"tamanho-buffer"},
#   {"client":"objetoClient", "sub_topic":"topico-subscribe","pub_topic":"topico-publish","func":"funcao", "size":"tamanho-buffer"}
# ]

threads = []

def on_connect(client, userdata, flags, rc):
    # verifica cada cliente na lista de clientes buscando
    # o cliente da lista q e igual ao passado pra funcao
    for c in clients:
        if (c['client'] == client):
            # quando acha pega o subtopico correspondente a ele
            sub_topic = c['sub_topic']
            client.subscribe(sub_topic)
            #print("subtopic: "+sub_topic)

def on_publish(client, userdata, mid):
   print("callback on_publish= "  ,mid) 

def on_message(client, userdata, msg):
    print("on_message")
    # verifica cada cliente na lista de clientes buscando
    # o cliente da lista q e igual ao passado pra funcao
    for c in clients:
        if (c['client'] == client):
            # quando acha aplica a funcao conforme no json de config
            func = c['func']
            size = c['size']
            pub_topic = c['pub_topic']

            # funcao de media
            if (func == "mean"):
                buffer = []
                saida = 0
                print("tamannho do buffer:", buffer)  
                print("id cliente: "+str(client.client_id))
                while len(buffer) < int(size):
                    data = str(msg.payload.decode("utf-8"))
                    buffer.append(int(data))
                saida = sum(buffer)/len(buffer)
                client.publish(pub_topic, saida)

            #funcao de soma
            if (func == "sum"):
                buffer = []
                saida = 0

                print("tamannho do buffer:", buffer)
                print("id cliente: "+str(client.client_id))

                while len(buffer) < int(size):
                    data = str(msg.payload.decode("utf-8"))
                    buffer.append(int(data))
                saida = sum(buffer)
                client.publish(pub_topic, saida)

# funcao para ler o arquivo config.json e retorna um dicionario
def read_json():
    with open('config.json') as file_json:
        data = json.load(file_json)
    return data

# funcao que cria os clientes em threads separadas
# adicionando para cada entrada "exec" do config.json 
# um novo cliente na lista "clients"
def create_connections(config):
    for exec in config.keys():
        client_id = exec
        client = paho.Client(client_id)
        
        # cria um dicionario auxiliar para salvar os 
        # dados do cliente junto com o objeto dele
        c_dic = {}
        c_dic['client'] = client
        c_dic['sub_topic'] = config[exec]['sub_topic']
        c_dic['pub_topic'] = config[exec]['pub_topic']
        c_dic['func'] = config[exec]['func']
        c_dic['size'] = config[exec]['size']

        # adiciona o dicionario auxiliar com todos dados 
        # do cliente na lista de clientes
        clients.append(c_dic)
        
        # faz a ligação das callbacks do objeto cliente com
        #  as callbacks sobrescritas no código
        client.on_publish = on_publish
        client.on_message = on_message
        client.on_connect = on_connect
        
        # inicia uma thread para representar o cliente
        t = threading.Thread(target=client_loop,args=(client,broker))
        threads.append(t)
        t.start()

# funcao que executa em cada thread
# realiza o client.loop
def client_loop(client, broker):
    print("running loop ")
    while True:
        if not client.connected_flag:
            print("Connecting to ",broker)
            client.connect(broker)
        # print("dentro ="+str(threading.currentThread().getName()))
        client.loop(0.01)

# faz o paho habilitar a flag "connected_flag" na classe Cliente
paho.Client.connected_flag=False

print("Criando conexoes ")
n_threads=threading.active_count() # captura o numero de threads ativas
print("numero de threads =",n_threads)

# le o json e inicia os clientes
config_file = read_json() 
create_connections(config_file)

n_threads=threading.active_count()
print("numero de threads =",n_threads) # captura o numero de threads ativas
print("iniciando loop principal")

# loop principal
while True:
    time.sleep(10)
    n_threads=threading.active_count()
    print("current threads =",n_threads)
