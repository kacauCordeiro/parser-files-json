import os
import json
import datetime
import time
import logging

host = 'host.com.br'
port = 22443
user = 'teste'
password = 'teste'
logger = logging.getLogger("importacao-serasa-one")
hdlr = logging.FileHandler("/var/log/relatorios/importacao-serasa-one.log")
formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)

logger.info("#------------------------------------------ Inicio ------------------------------------------#")

import_path = '/home/jaula/serasa/one/importacao/pendentes/'
move_path = '/home/jaula/serasa/one/importacao/historico/'
path_arquivo = '/home/jaula/serasa/one/importacao/pendentes/'
arquivos_csv = '/home/jaula/serasa/one/importacao/processados/'
error_path = '/home/jaula/serasa/one/importacao/erro/'

now = datetime.datetime.now()
extensoes_permitidas = []
chunk_size = 200000

lista_de_campos = ['cd_uuid','cd_offer','ds_partner_document','ds_partner_key','ds_partner_group','cd_event',
                   'ds_event_origin','ts_event','ls_debts','ls_instalments','vl_offer','pc_discount_offer','vl_current',
                   'cd_offer_signature','cd_id','cd_agreement','ds_contract','data','updated_at','op']

def dados_ordenados(lista_de_campos, array_de_dados):
    """Orde os dados da linha de acordo com os campos mapeados. Se a key não existir, o campo será inserido vazio."""
    array_linhas = []
    linha = []
    for dado_linha in array_de_dados:
        linha = []
        for campo in lista_de_campos:
            try:
                linha.append(dado_linha[campo])
            except:
                linha.append("")
        array_linhas.append(linha)
    return array_linhas            
    
def agrupador_evento(array_linhas, filtrar_posicao=5):
    """Recebe os dados ordenadados e ordem do campo para realizar o filtro."""
    eventos = {}
   
    for linha in array_linhas:
        evento_existe = False
        cd_event = linha[filtrar_posicao]
        
        if cd_event in eventos.keys():
            evento_existe = True
                
        if not evento_existe:
            eventos[cd_event] = {"codigo": cd_event, "linhas": []}
        
        eventos[cd_event]["linhas"].append(linha)
    return eventos
    
try:
    
    logger.info("Importando arquivos")
    
    logger.info("Iniciando parser dos arquivos.")
    arquivos = os.listdir(path_arquivo)
    
    for arquivo in arquivos:
        try:
            logger.info("Processando arquivo: {}".format(arquivo, ))
            dados = open(os.path.join(path_arquivo, arquivo), "r", encoding="utf-8")
            data_dict = json.load(dados)
            logger.info("#JSON-PARSER REALIZADO")
            array_linhas = dados_ordenados(lista_de_campos, data_dict)
            logger.info("#DADOS-ORDENADOS")
            eventos = agrupador_evento(array_linhas)
            logger.info("#EVENTOS-PARSER REALIZADO")
            
            for e in eventos.keys():
                linhas_evento = eventos[e]['linhas']
                linhas_evento = [linhas_evento[i:i + chunk_size] for i in range(0, len(linhas_evento), chunk_size)]
                for i, array in enumerate(linhas_evento):
                    nm_arquivo_csv = arquivo.replace(".json", "") + "_{}_{}.csv".format(eventos[e]['codigo'], i)
                    with open(os.path.join(arquivos_csv, nm_arquivo_csv), "w+", encoding="utf-8") as f:
                        f.write(';'.join(map(str, lista_de_campos)))
                        for linha in array:
                            linha = ';'.join(map(str, linha))
                            f.write("{}\r\n".format(linha, ))
                    logger.info("Arquivo gerado: {}".format(nm_arquivo_csv, ))
                    f.close()
            logger.info("Processando Finalizado com Sucesso: {}".format(arquivo, ))
            os.rename(import_path + arquivo, move_path + arquivo)
        except Exception as e:
            logger.info("Falha no processando: {}".format(arquivo, ))
            os.rename(import_path + arquivo, error_path + arquivo)
            print(str(e))
        
except Exception as e:
    print(str(e))
    

