from elasticsearch import Elasticsearch
import config_file


def delete_all_indices(shard_info_result,ES_client,index_type):
    data= shard_info_result['hits']['hits'][0]['_source']

    for shard in data[index_type]:
        if(ES_client.indices.exists(index=shard)):
            ES_client.indices.delete(index=shard)

    return



def main():
    ES_client= Elasticsearch(config_file.elasticsearch_address)
    print("elasticsearch server is running",ES_client.ping())

    shard_info_result= ES_client.search(index=config_file.info_index_name, body={ 'query': { 'match_all':{}}})

    delete_all_indices(shard_info_result,ES_client,config_file.permanet_shard_list_name)
    delete_all_indices(shard_info_result,ES_client,config_file.temporary_shard_list_name)
    delete_all_indices(shard_info_result,ES_client,config_file.next_iter_shard_list_name)




if __name__ == "__main__":
    main()
