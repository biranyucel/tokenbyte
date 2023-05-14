### libaries
import requests
import os
import pandas as pd
import numpy as np
from datetime import datetime


### GraphQL Queries 
query_to_vesting = """
      query transfersToVesting($limit: Int!, $nextCursor: String, $tokenAddress: Address, $address: Identity ) {
        transfersToVesting: TokenTransfers(
          input: {
            filter: {
              tokenAddress: {_eq: $tokenAddress},
              from: {_eq:  $address}
            },
            blockchain: ethereum,
            limit: $limit,
            cursor: $nextCursor,
            order: { blockTimestamp: DESC }
          }
        ) {
          pageInfo {
            nextCursor
            prevCursor
          }
          TokenTransfer {
          amount
          blockNumber
          blockTimestamp
          from {
            addresses
          }
          to {
            addresses
          }
          tokenAddress
          transactionHash
        }
        }
      }
      """


query_from_vesting = """
      query transfersFromVesting($limit: Int!, $nextCursor: String, $tokenAddress: Address, $address: Identity ) {
        transfersFromVesting: TokenTransfers(
          input: {
            filter: {
              tokenAddress: {_eq: $tokenAddress},
              from: {_eq:  $address}
            },
            blockchain: ethereum,
            limit: $limit,
            cursor: $nextCursor,
            order: { blockTimestamp: DESC }
          }
        ) {
          pageInfo {
            nextCursor
            prevCursor
          }
          TokenTransfer {
          amount
          blockNumber
          blockTimestamp
          from {
            addresses
          }
          to {
            addresses
          }
          tokenAddress
          transactionHash
        }
        }
      }
      """


### FUNCTION 

def get_relevant_time_range(df): 

    # Convert the object type to datetime64
    min_value = np.min(df.reset_index().rounded_timestamp.values.astype('datetime64[D]'))

    # Get the current date
    current_date = np.datetime64(datetime.now(), 'D')

    # Calculate the difference in days between the minimum value and the current date
    difference = (current_date - min_value).astype(int)

    return difference


### SCRIPT

# address = input("Please enter the underlying Token which will be locked into the veContract: ") #0xC128a9954e6c874eA3d62ce62B468bA073093F25  #veBAL
# tokenAddress = input("Please enter the veContractToken: ") #0x5c6Ee304399DBdB9C8Ef030aB642B10820DB8F56  # 80BAL-20WETH

# USER INPUTS
def query(address,tokenAddress,api_key):
    
    # api_key = os.getenv('AIRSTACK_API_KEY')

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    

    # tokenAddress = '0x5c6Ee304399DBdB9C8Ef030aB642B10820DB8F56' #B-80BAL-20WETH
    # address = "0xC128a9954e6c874eA3d62ce62B468bA073093F25"


    # QUERY DATA
    file_path_tuple_list = [('transfersToVesting',query_to_vesting,"./outputs/query_results_to_vesting.csv"),  ('transfersFromVesting', query_from_vesting, "./outputs/query_results_from_vesting.csv")]

    for graphql_name, query, file_path in file_path_tuple_list: 

        
        # Create an empty DataFrame
        df = pd.DataFrame()

        # Create a new file with header
        df.to_csv(file_path, index=False)

        # Define the file path

        switch = True

        nextCursor = "" 

        while True:

            variables = {
                "limit": 200,
                "nextCursor": nextCursor,
                "tokenAddress": tokenAddress,
                "address": address
            }

            response = requests.post("https://api.airstack.xyz/gql", headers=headers, json={"query": query, "variables": variables})
            data = response.json()

            # Extract the token transfer data
            transfers = data["data"][graphql_name]["TokenTransfer"] #change

            # Convert the data to a DataFrame
            df_temp = pd.json_normalize(transfers)
            df_temp['from.addresses'] = df_temp['from.addresses'].apply(lambda x: x[0])
            df_temp['to.addresses'] = df_temp['to.addresses'].apply(lambda x: x[0])
            df_temp['amount'] = df_temp['amount'].astype(float)


            # Append the DataFrame to the main DataFrame
            df = df.append(df_temp, ignore_index=False)

            # Get the next cursor for the next page
            pageInfo = data["data"][graphql_name]["pageInfo"]
            nextCursor = pageInfo["nextCursor"]

            # Exit the loop if there is no next cursor
            if not nextCursor:
                break

            # Save the DataFrame to a CSV file incrementally
            df.to_csv(file_path, index=False, mode='a', header=switch)

            # Set df_to_exists flag to True after the first iteration
            switch = False

    ### Staking 
    # load df's 
    df_results_from_vesting = pd.read_csv(file_path_tuple_list[0][2])
    df_results_to_vesting = pd.read_csv(file_path_tuple_list[1][2])


    # from 
    df_from_slt = df_results_from_vesting[['blockTimestamp','from.addresses','amount']]
    df_from_slt.rename(columns={'from.addresses': 'address'}, inplace=True)


    # to 
    df_to_slt = df_results_to_vesting[['blockTimestamp','to.addresses','amount']]
    df_to_slt['amount'] = df_to_slt['amount'].multiply(-1)
    df_to_slt.rename(columns={'to.addresses': 'address'}, inplace=True)


    ### df_concat

    df_concat = pd.concat([df_from_slt, df_to_slt], axis=0)
    df_concat.reset_index(inplace=True)
    df_concat['blockTimestamp'] = df_concat['blockTimestamp'].apply(lambda x : datetime.strptime( x , '%Y-%m-%dT%H:%M:%SZ'))
    df_concat['rounded_timestamp'] = df_concat['blockTimestamp'].dt.round('D')




    ### PRICE DATA
    
    difference = get_relevant_time_range(df_concat)

    difference = get_relevant_time_range(df_concat)
    coingecko_url = f"https://api.coingecko.com/api/v3/coins/balancer/market_chart?vs_currency=usd&days={difference}"
    data = requests.get(coingecko_url).json()




    ### 

    df_price = pd.DataFrame(data['prices'], columns=['rounded_timestamp', 'price_usd'])
    df_price.rounded_timestamp = pd.to_datetime(df_price.rounded_timestamp, unit='ms')

    df_combined = df_price.merge(df_concat, on='rounded_timestamp').drop(["index",'blockTimestamp'], axis=1)

    return df_combined
    