import bs4
import urllib.request
import config_file
import re
from elasticsearch import Elasticsearch
import time
import requests


def get_shard_name(shard_info_result,starting_character,index_type):
    no_of_shards= shard_info_result['hits']['hits'][0]['_source']['no_of_indices']

    for i in range(no_of_shards):
        if(((shard_info_result['hits']['hits'][0]['_source']['shard_open_index'][i])<=starting_character) and (shard_info_result['hits']['hits'][0]['_source']['shard_close_index'][i]>=starting_character)):
            if(index_type=='permanet'):
                return (shard_info_result['hits']['hits'][0]['_source']['shard_list'][i])
            elif(index_type=='temporary'):
                return (shard_info_result['hits']['hits'][0]['_source']['temp_shard_list'][i])
            else:
                return (shard_info_result['hits']['hits'][0]['_source']['next_temp_shard_name'][i])


def prepare_indices(shard_info_result,ES_client,index_type):
    data= shard_info_result['hits']['hits'][0]['_source']
    mapping_body= {
    "properties":{
    "url":{
    "type": "keyword"
    }
    }
    }
    for shard in data[index_type]:
        ES_client.indices.create(index=shard)
        ES_client.indices.put_mapping(index=shard,body=mapping_body)
    return


def get_size_of_perma_index(shard_info_result,ES_client):
    result=0

    for shard in (shard_info_result['hits']['hits'][0]['_source']['shard_list']):
        result_str= ES_client.cat.count(index=shard)
        str_arr= result_str.split()
        result= result + int(str_arr[2])

    return int(result)


def v_main():
    ES_client= Elasticsearch([config_file.elasticsearch_address])
    print("ES server is running ",ES_client.ping())

    shard_info_result= ES_client.search(index=config_file.info_index_name, body={ 'query': { 'match_all':{}}})
    shard_name_to_start= get_shard_name(shard_info_result,'a','temporary')
    shard_data= shard_info_result['hits']['hits'][0]['_source']
    print("shard name to start with is ",shard_name_to_start)
    print("shard_info_result", shard_info_result['hits']['hits'][0]['_source'])

    total_num= get_size_of_perma_index(shard_info_result,ES_client)
    print('total docs in permanet database are :',total_num)



def main():
    ES_client= Elasticsearch([config_file.elasticsearch_address])
    print("ES server is running ",ES_client.ping())

    shard_info_result= ES_client.search(index=config_file.info_index_name, body={ 'query': { 'match_all':{}}})

    #start search and keep scroll context alive
    body ={
        "url":config_file.seed
    }

    prepare_indices(shard_info_result,ES_client,config_file.temporary_shard_list_name)
    prepare_indices(shard_info_result,ES_client,config_file.permanet_shard_list_name)
    shard_name_to_start= get_shard_name(shard_info_result,'a','temporary')
    ES_client.index(index=shard_name_to_start, body=body)
    ES_client.indices.refresh(index=[shard_name_to_start])


    #start scrolling the current_temp_shard
    search_body={
    'size': 1,
    'query':{
    'match_all': {}
    }
    }


    while(get_size_of_perma_index(shard_info_result,ES_client)<50):

        print('stats before iteration starts: ', ES_client.cat.count(index='temp_shard_1'))
        context = ES_client.search(index='temp_shard_1', body= search_body, scroll='1h')
        print('result length.....................................:::',len(context['hits']['hits']))
        prepare_indices(shard_info_result,ES_client,config_file.next_iter_shard_list_name)
        print('starts before iteration actually starts',ES_client.indices.get_settings(index='temp_shard_1'))

        while(len(context['hits']['hits'])):

            print('printing material')
            print(context['hits']['hits'][0])

            url= context['hits']['hits'][0]['_source']['url']
            print("the url to hit next is: ",url)
            # print("the url is",url)
            if(not url):
                print('exited because url is invalid in temp_shard_1')
                scroll_id= context['_scroll_id']
                context= ES_client.scroll(scroll_id=scroll_id,scroll='1h')
                continue

            check_search_body= {
            "query": {
            "bool": {
            "must":{
            "match":{
            "url":url
            }
            }
            }
            }
            }

            shard_name_to_search=get_shard_name(shard_info_result,'a','permanet')
            perma_confirm_res=ES_client.search(index=shard_name_to_search,body=check_search_body)
            len_from_perma_index= len(perma_confirm_res['hits']['hits'])

            if(len_from_perma_index>0):
                # print('duplicate found in permanet index')
                scroll_id=context['_scroll_id']
                context= ES_client.scroll(scroll_id=scroll_id,scroll='1h')
                continue


            #here check is only done i

            response= requests.get(config_file.seed)
            webContent = response.text
            # print(webContent)

            soup = bs4.BeautifulSoup(webContent,'html.parser')
            for link in soup.find_all('a'):
                url_text=link.get('href')
                print('url_text is here: ',url_text)
                # print("each url_text is",url_text)
                shard_to_insert= get_shard_name(shard_info_result,'a','next_iter')
                shard_name_temp= get_shard_name(shard_info_result,'a', 'temporary')
                shard_name_perm= get_shard_name(shard_info_result,'a','permanet')

                if(not url_text):
                    print('exited here becuase url_text is invalid')
                    continue

                insert_body={
                "query":{
                "bool":{
                "must":{
                "match":{
                "url": url_text
                }
                }
                }
                }
                }


                #check in perma index_type
                res_from_perma= ES_client.search(index=shard_name_perm,body=insert_body)
                len_from_perma= len(res_from_perma['hits']['hits'])
                # chek in temporar index_type
                res_from_temp= ES_client.search(index=shard_name_temp,body=insert_body)
                len_from_temp= len(res_from_temp['hits']['hits'])

                if(len_from_temp or len_from_perma):
                    print('exited because link already found')
                    continue

                print("New link found ",url_text)
                ES_client.index(index=shard_to_insert,body={'url':url_text})
                print('new link iducted in next_iter')



            # next batch iteration starts
            scroll_id= context['_scroll_id']
            context=ES_client.scroll(scroll_id=scroll_id,scroll='1h')


            #scroll ends here for current iteration
            # next batch prepartion is done here
            final_shard_name=get_shard_name(shard_info_result,'a','permanet')
            ES_client.index(index=final_shard_name,body={'url':url})
            ES_client.indices.refresh(index=[final_shard_name])
            print("link enters db is: ",body)


        ES_client.clear_scroll(scroll_id=context['_scroll_id'])
        print('stats before cloning and deleting..')
        print("Next_iter  stats", ES_client.cat.count(index='next_iter_shard_1'))
        print('temp_shard stats', ES_client.cat.count(index='temp_shard_1'))
        ES_client.indices.delete(index=['temp_shard_1'])
        ES_client.indices.put_settings( index='next_iter_shard_1', body={"settings": {"index.blocks.write": True }})
        ES_client.indices.clone(index='next_iter_shard_1',target='temp_shard_1',)
        ES_client.indices.put_settings( index='temp_shard_1', body={"settings": {"index.blocks.write": False }})
        ES_client.indices.delete(index=['next_iter_shard_1'])


        print('stats after cloning and deleting ')
        print('temp_shard stats', ES_client.cat.count(index='temp_shard_1'))
        print("cloning and deleting done")
        time.sleep(2)


if __name__ == "__main__":
    main()
