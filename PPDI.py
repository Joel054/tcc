#while :; do  mosquitto_pub -t "c0/temp" -m "$(( $RANDOM % 3 + 21 ))"; done


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
#   {"client":"objetoClient", "sub_topic":"topico-subscribe","pub_topic":"topico-publish","func":"funcao", "size":"tamanho-buffer", "buffer":"buffer"},
#   {"client":"objetoClient", "sub_topic":"topico-subscribe","pub_topic":"topico-publish","func":"funcao", "size":"tamanho-buffer", "buffer":"buffer"}
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
            print("subtopic: "+sub_topic)

def on_publish(client, userdata, mid):
   print("callback on_publish= "+ str(mid)+" thread: "+str(threading.currentThread().getName())) 

def on_message(client, userdata, msg):
    print("on_message - thread: "+str(threading.currentThread().getName()))
    # verifica cada cliente na lista de clientes buscando
    # o cliente da lista q e igual ao passado pra funcao
    for c in clients:
        if (c['client'] == client):
            # quando acha aplica a funcao conforme no json de config
            func = c['func']
            size = c['size']
            pub_topic = c['pub_topic']

            # funcao de media
            if (func == "media"):
                if(len(c['buffer']) < int(size)):
                    data = str(msg.payload.decode("utf-8"))
                    c['buffer'].append(int(data))
                    print("buffer: "+ str(c['buffer']) +" thread: "+str(threading.currentThread().getName()))
                else:
                    saida = sum(c['buffer'])/len(c['buffer'])
                    print("publish topic: "+str(pub_topic)+" saida: "+str(saida))
                    client.publish(pub_topic, saida)
                    c['buffer'] = []


            #funcao de soma
            if (func == "soma"):
                if(len(c['buffer']) < int(size)):
                    data = str(msg.payload.decode("utf-8"))
                    c['buffer'].append(int(data))
                    print("buffer: "+ str(c['buffer']) +" thread: "+str(threading.currentThread().getName()))
                else:
                    saida = sum(c['buffer'])
                    print("publish topic: "+str(pub_topic)+" saida: "+str(saida))
                    client.publish(pub_topic, saida)
                    c['buffer'] = []

            
            #controle esq <-> dir
            if (func == "direcao"):
                if(len(c['buffer']) < int(size)):
                    data = str(msg.payload.decode("utf-8"))
                    c['buffer'].append(int(data))
                    print("buffer: "+ str(c['buffer']) +" thread: "+str(threading.currentThread().getName()))
                else:
                    saida = sum(c['buffer'])/len(c['buffer'])
                    print("publish topic: "+str(pub_topic)+" saida: "+str(saida))
                    client.publish(pub_topic, saida)
                    c['buffer'] = []


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
        c_dic['buffer'] = []


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
    client.connect(broker)
    client.run_flag=True
    #client.loop_forever()

    while client.run_flag:
        client.loop()
    
    print("Desconectando do broker: ",broker)

def kill_all():
    for c in clients:
        c["client"].run_flag=False

# faz o paho habilitar a flag "connected_flag" na classe Cliente
#paho.Client.connected_flag=False
#paho.Client.run_flag=False

print("Criando conexoes ")
n_threads=threading.active_count() # captura o numero de threads ativas
print("numero de threads =",n_threads)

# le o json e inicia os clientes
config_file = read_json() 
create_connections(config_file)

n_threads=threading.active_count()
print("numero de threads =",n_threads) # captura o numero de threads ativas
print("iniciando loop principal")

# simular/testar matar as threads
# quando o count == 30 (passou 30 segundos na main) mata as threads
count = 0

# loop principal
try:
    while True:
        time.sleep(1)
        count+=1
        if count == 30:
            kill_all()
            break

except KeyboardInterrupt:
    print("mantando a aplicacao")
    kill_all()

print("Finish aplication")