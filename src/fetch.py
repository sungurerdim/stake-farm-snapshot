from time import sleep, time
from web3 import Web3

import json

import requests
import pandas as pd
from urllib.parse import urlparse
from sys import exit

from tqdm import tqdm

from .utils import find_file, df_to_csv, checkAddress, download_file_again


def createRequestSession():
    max_retries = 3

    request_session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(max_retries=max_retries)
    request_session.mount("http://", adapter)
    request_session.mount("https://", adapter)

    return request_session


def getContractABI(contractAddress_, temp_settings):
    delay = temp_settings["API_CALL_DELAY"]

    max_retries = 10
    retry = 0

    module = "contract"
    action = "getabi"
    address = contractAddress_
    
    chainid = temp_settings["CHAIN_ID"]
    apikey = temp_settings["API_KEY"]
    
    params = {
        "chainid": chainid,
        "apikey": apikey,
        "module": module,
        "action": action,
        "address": address,
    }

    while retry <= max_retries:
        sleep(delay)
        retry += 1

        try:
            response = make_http_request(temp_settings["API_URL"], target_key="result", parameters=params, headers=None)

            if not response: return None

            abi = json.loads(response["data"])

            return abi
        except Exception as err:
            print(f"! Error: Failed to fetch contract abi. Retrying in {delay} seconds...")
            print()
            print(err)
            print()

            continue
    
    print()
    print(f"! Error: Failed to fetch contract abi. Reached max retry limit {max_retries}. Terminating script...")
    print()

    return None


def epochToBlockNumber(targetEpoch_, temp_settings, closest_ = "after"):
    if not targetEpoch_: return 0

    delay = 3

    module = "block"
    action = "getblocknobytime"
    timestamp = int(targetEpoch_)
    closest = closest_
    
    chainid = temp_settings["CHAIN_ID"]
    apikey = temp_settings["API_KEY"]
    
    params = {
        "chainid": chainid,
        "apikey": apikey,
        "module": module,
        "action": action,
        "timestamp": timestamp,
        "closest": closest,
    }

    while True:
        try:
            response = make_http_request(temp_settings["API_URL"], target_key="result", parameters=params, headers=None)["data"]

            if not response:
                continue

            response = int(response)
            break
        except (ConnectionError, TimeoutError, ValueError):
            # API request failed, wait and try again
            # print()
            # print(f"! Error: Failed to get block number for epoch timestamp {targetEpoch_}. Retrying in {delay} seconds...")
            sleep(delay)

    return response


def web3Connection(rpcURL, delay=3):
    if not rpcURL: return None
    conn = None

    max_retries = 3
    r = 1
    while True:
        conn = Web3(Web3.HTTPProvider(rpcURL))

        if conn.is_connected(): return conn
        
        print("! Error: Connection attempt to", rpcURL, "failed. Retry count:", f"{r}/{max_retries}")
        sleep(delay)

        r += 1

        if r > max_retries: return None

def get_contract_creation_timestamp(ofThisContract, temp_settings):

    if ofThisContract is None: return None

    ofThisContract = checkAddress(ofThisContract)
    
    sleep(temp_settings["API_CALL_DELAY"])

    module = "account"
    action = "txlistinternal"
    address = ofThisContract
    startblock = 0
    sort = "asc"

    chainid = temp_settings["CHAIN_ID"]
    apikey = temp_settings["API_KEY"]
    
    params = {
        "chainid": chainid,
        "apikey": apikey,
        "module": module,
        "action": action,
        "address": address,
        "startblock": startblock,
        "sort": sort,
    }

    result = make_http_request(temp_settings["API_URL"], target_key="result", parameters=params, headers=None)

    if not result: return None

    result_data = result["data"]
    first_item = result_data[0]

    first_timestamp = first_item["timeStamp"]

    return int(first_timestamp)

def fetch_lp_history(lp_contract, token_contract, base_snapshot_timestamps, temp_settings):
    if lp_contract is None or token_contract is None or base_snapshot_timestamps is None or temp_settings is None:
        return None
    
    contract_creation_timestamp = get_contract_creation_timestamp(lp_contract, temp_settings)
    filtered_snapshot_timestamps = base_snapshot_timestamps[base_snapshot_timestamps >= contract_creation_timestamp]

    if filtered_snapshot_timestamps is None or len(filtered_snapshot_timestamps) < 1: return None

    print(f"** Checking historical LP values for {lp_contract}")

    lp_history_file_name = f"LP_HISTORY_{lp_contract}.csv"
    lp_history_csv = find_file(lp_history_file_name)

    DF_LP_HISTORY_OLD = None
    DF_LP_HISTORY_NEW = pd.DataFrame(index=filtered_snapshot_timestamps, columns=["lpAmount", "tokenAmount"])

    if lp_history_csv:
        DF_LP_HISTORY_OLD = pd.read_csv(lp_history_csv)

        DF_LP_HISTORY_OLD["timeStamp"] = DF_LP_HISTORY_OLD["timeStamp"].apply(int)
        DF_LP_HISTORY_OLD["lpAmount"] = DF_LP_HISTORY_OLD["lpAmount"].apply(lambda x: int(x) if pd.notna(x) else x)
        DF_LP_HISTORY_OLD["tokenAmount"] = DF_LP_HISTORY_OLD["tokenAmount"].apply(lambda x: int(x) if pd.notna(x) else x)

        DF_LP_HISTORY_OLD = DF_LP_HISTORY_OLD.set_index("timeStamp")

        DF_LP_HISTORY = pd.concat([DF_LP_HISTORY_OLD, DF_LP_HISTORY_NEW[~DF_LP_HISTORY_NEW.index.isin(DF_LP_HISTORY_OLD.index)]]).loc[filtered_snapshot_timestamps,:]
    else:
        DF_LP_HISTORY = DF_LP_HISTORY_NEW
    
    timestamps_of_missing_values = None
    timestamps_of_missing_values = DF_LP_HISTORY[DF_LP_HISTORY.isnull().any(axis=1)].index
    missing_values_count = len(timestamps_of_missing_values)

    if  missing_values_count > 0:
        print(f"*** Fetching contract ABI for {lp_contract}")
        contract_abi = getContractABI(lp_contract, temp_settings)

        if contract_abi is None: exit()

        RPC_NODES = temp_settings["RPC_NODES"]
        CURRENT_RPC_INDEX = -1

        MAX_RPC_TRY = temp_settings["MAX_RPC_TRY"] * len(RPC_NODES)
        CUR_RPC_TRY = 0

        token0 = None
        token1 = None
        reserve_index = None

        while True:
            CUR_RPC_TRY += 1

            if CUR_RPC_TRY > MAX_RPC_TRY:
                raise Exception(f"*** !!! --- All RPC nodes failed after {MAX_RPC_TRY} attempts --- !!!")

            sleep(temp_settings["API_CALL_DELAY"])

            print()
            print("*** Connecting to RPC node")

            web3, CURRENT_RPC_INDEX = setRPC(RPC_NODES, CURRENT_RPC_INDEX)

            CUR_RPC_NODE = RPC_NODES[CURRENT_RPC_INDEX]
            CUR_RPC_NODE = urlparse(CUR_RPC_NODE).netloc            

            print(f"*** Active RPC node: {CUR_RPC_NODE}")

            print(f"*** Creating contract instance for {lp_contract}")
            contract_instance = createContractInstance(web3, lp_contract, contract_abi)
            
            if token0 is None:
                print(f"*** Getting contract of first token in LP ({lp_contract})")
                token0 = contract_instance.functions.token0().call({'from': lp_contract})
                token0 = checkAddress(token0)

                if token0 is None:
                    print(f"**** ! Error: contract address of token0 is invalid - RPC node: {CUR_RPC_NODE}, Result: {token0}, switching to another RPC node...")
                    continue
            
            sleep(temp_settings["API_CALL_DELAY"])

            if token1 is None:
                print(f"*** Getting contract of second token in LP ({lp_contract})")
                token1 = contract_instance.functions.token1().call({'from': lp_contract})

                token1 = checkAddress(token1)

                if token1 is None:
                    print(f"**** ! Error: contract address of token1 is invalid - RPC node: {CUR_RPC_NODE}, Result: {token1}, switching to another RPC node...")
                    continue
            
            sleep(temp_settings["API_CALL_DELAY"])
            
            if reserve_index is None:
                if token0 == token_contract:
                    reserve_index = 0
                elif token1 == token_contract:
                    reserve_index = 1
                else:
                    print(f"**** Skipping LP token ({lp_contract}), target token is not a part of the pair")
                    return None
            
            timestamps_of_missing_values = None
            timestamps_of_missing_values = DF_LP_HISTORY[DF_LP_HISTORY.isnull().any(axis=1)].index
            timestamps_of_missing_values = DF_LP_HISTORY[DF_LP_HISTORY.isnull().any(axis=1)].index
            missing_values_count = len(timestamps_of_missing_values)

            if missing_values_count > 0:
                print(f"*** Found {missing_values_count} missing LP values for {lp_contract}")
                print(f"*** Fetching missing historical LP pair amounts for {lp_contract}")
                
                for NEXT_LP_TIMESTAMP in tqdm(timestamps_of_missing_values, unit="values"): #, colour="green"
                    NEXT_LP_BLOCK = epochToBlockNumber(NEXT_LP_TIMESTAMP, temp_settings)

                    # -----------------------------------------------------------------------------

                    sleep(temp_settings["API_CALL_DELAY"])

                    try:
                        # totalSupply() call
                        totalSupply = contract_instance.functions.totalSupply().call({'from': lp_contract}, block_identifier=NEXT_LP_BLOCK)
                    except:
                        continue

                    sleep(temp_settings["API_CALL_DELAY"])

                    try:
                        # getReserves() call
                        getReserves = contract_instance.functions.getReserves().call({'from': lp_contract}, block_identifier=NEXT_LP_BLOCK)[reserve_index]
                    except:
                        continue

                    # -----------------------------------------------------------------------------

                    total_lp_amount = totalSupply
                    total_tokens_in_lps = getReserves

                    DF_LP_HISTORY.loc[NEXT_LP_TIMESTAMP, 'lpAmount'] = total_lp_amount
                    DF_LP_HISTORY.loc[NEXT_LP_TIMESTAMP, 'tokenAmount'] = total_tokens_in_lps

                    df_to_csv(DF_LP_HISTORY, lp_history_file_name, 'timeStamp', ',')
                
                timestamps_of_missing_values = None
                timestamps_of_missing_values = DF_LP_HISTORY[DF_LP_HISTORY.isnull().any(axis=1)].index

                missing_values_count = len(timestamps_of_missing_values)

                if missing_values_count > 0:
                    print(f"**** {missing_values_count} values are still missing, switching to another RPC node...")
                    continue
                else:
                    break

            else:
                print(f"*** We already have the most up-to-date data")
                break
        
        df_to_csv(DF_LP_HISTORY, lp_history_file_name, 'timeStamp', ',')
    else:
        print(f"*** We already have the most up-to-date data")

    return DF_LP_HISTORY


def make_http_request(target_url, target_key="result", parameters=None, headers=None):
    session = createRequestSession()
    session.keep_alive = 5

    timeout = 30
    retry_delay = 10
    max_retries = 5
    retry = 0
    
    while retry < max_retries:
        try:
            response = session.get(target_url, params=parameters, headers=headers, timeout=timeout)
            
            # print()
            # print(f"make_http_request - Full URL: {response.url}")
            # print(f"Status code: {response.status_code}")
            # print(f"Raw response: {response}")
            # print(f"Raw response JSON: {response.json()}")
            # print()

            response.raise_for_status()  # Will raise an HTTPError for bad responses (4xx or 5xx)

            try:
                # Attempt to parse JSON and access the target key
                json_data = response.json()

                if target_key:
                    if target_key in json_data:
                        return {
                            "status_code": response.status_code,
                            "data": json_data[target_key],
                        }
                    else:
                        raise KeyError(f"Key '{target_key}' not found in the response JSON.")
                else:
                    return {
                        "status_code": response.status_code,
                        "data": json_data,
                    }
            except ValueError:
                # JSON decoding failed
                print()
                print("Error: Unable to decode JSON response. Returning raw content.")
                return {
                    "status_code": response.status_code,
                    "data": response.content,  # Return raw content if JSON parsing fails
                }

        except requests.exceptions.Timeout:
            print(f"Timeout occurred. Retrying in {retry_delay} seconds... ({retry + 1}/{max_retries})")
        except requests.exceptions.TooManyRedirects:
            print("Error: Too many redirects. Check the URL. Retrying in {retry_delay} seconds... ({retry + 1}/{max_retries})")
            break  # Stop retrying if this error occurs
        except requests.exceptions.RequestException as ex:
            print(f"Error: {type(ex).__name__} occurred: {ex}. Retrying in {retry_delay} seconds... ({retry + 1}/{max_retries})")
            # Handle specific errors or fall back to a general case
        except KeyError as ke:
            # Handle KeyError specifically if the target key is not in the JSON
            print(f"Key error: {ke}. Returning raw content. Retrying in {retry_delay} seconds... ({retry + 1}/{max_retries})")
            return {
                "status_code": response.status_code,
                "data": response.content,
            }
        except Exception as ex:
            # Catch any other exceptions not anticipated
            print(f"Unexpected error: {type(ex).__name__} occurred: {ex}. Retrying in {retry_delay} seconds... ({retry + 1}/{max_retries})")
            raise  # Re-raise unexpected exceptions to avoid silent failures

        # Check if max retries reached
        if retry >= max_retries - 1:
            print("Max retries reached. Halting script.")
            raise requests.exceptions.RequestException("Max retries reached. Cannot fetch data.")

        retry += 1
        sleep(retry_delay)

def getContractOwner(ofThisContract, temp_settings):
    if not ofThisContract: return None

    module = "contract"
    action = "getcontractcreation"
    contractaddresses = ofThisContract
    sort = "asc"

    chainid = temp_settings["CHAIN_ID"]
    apikey = temp_settings["API_KEY"]
    
    params = {
        "chainid": chainid,
        "apikey": apikey,
        "module": module,
        "action": action,
        "contractaddresses": contractaddresses,
        "sort": sort,
    }

    result = make_http_request(temp_settings["API_URL"], target_key="result", parameters=params, headers=None)

    if not result:
        result = make_http_request(temp_settings["API_URL"], target_key="result", parameters=params, headers=None)

        if not result: return None

    try:
        return result["data"][0]["contractCreator"]
    except:
        return None
    

def fetch_pool_txns( pool, temp_settings ):
    # Max. number of txns in a single API call
    batch_size = 10000

    pool_name, pool_contract, pool_multiplier, pool_contract_owner, target_token, lp_history = pool
    
    if not pool_contract: return

    # ------------------------------------------------------------------

    END_BLOCK_NUMBER = temp_settings["SNAPSHOT_BLOCK_NUMBER"]

    txn_export_file_name = f"{pool_contract}.csv"
    txn_export_csv = find_file(txn_export_file_name)

    column_names = ["blockNumber", "timeStamp", "from", "to", "value"]

    if txn_export_csv:
        DF_POOL_TXN_HISTORY = pd.read_csv(txn_export_csv)
    else:
        DF_POOL_TXN_HISTORY = pd.DataFrame()

    if DF_POOL_TXN_HISTORY.empty:
        START_BLOCK_NUMBER = 0
    else:
        START_BLOCK_NUMBER = int(DF_POOL_TXN_HISTORY.iloc[-1]["blockNumber"])
    
    print("* Checking transactions")

    LIST_DF_PARTIAL_TXNS = [DF_POOL_TXN_HISTORY]

    if START_BLOCK_NUMBER <= END_BLOCK_NUMBER:
        print("* Fetching new transactions")
    
    while START_BLOCK_NUMBER <= END_BLOCK_NUMBER:

        txn_list = getTokenTxnList(pool_contract, target_token, temp_settings, START_BLOCK_NUMBER, END_BLOCK_NUMBER)

        if (txn_list is None) or (len(txn_list) == 0): break

        DF_PARTIAL_TXNS = pd.DataFrame(txn_list)

        LIST_DF_PARTIAL_TXNS.append(DF_PARTIAL_TXNS)

        if len(txn_list) < batch_size: break

        START_BLOCK_NUMBER = int(DF_PARTIAL_TXNS.iloc[-1]["blockNumber"])

        sleep(temp_settings["API_CALL_DELAY"])

    if len(LIST_DF_PARTIAL_TXNS) == 0: return DF_POOL_TXN_HISTORY

    old_txn_count = DF_POOL_TXN_HISTORY.shape[0]

    DF_POOL_TXN_HISTORY = pd.concat(LIST_DF_PARTIAL_TXNS)
    
    if 'input' in DF_POOL_TXN_HISTORY.columns:
        DF_POOL_TXN_HISTORY = DF_POOL_TXN_HISTORY.drop(['input'], axis=1)
    
    if 'confirmations' in DF_POOL_TXN_HISTORY.columns:
        DF_POOL_TXN_HISTORY = DF_POOL_TXN_HISTORY.drop(['confirmations'], axis=1)
    
    DF_POOL_TXN_HISTORY = DF_POOL_TXN_HISTORY.astype(str)
    DF_POOL_TXN_HISTORY["from"] = DF_POOL_TXN_HISTORY["from"].apply(checkAddress)
    DF_POOL_TXN_HISTORY["to"] = DF_POOL_TXN_HISTORY["to"].apply(checkAddress)
    DF_POOL_TXN_HISTORY = DF_POOL_TXN_HISTORY.drop_duplicates(keep="first")

    new_txn_count = DF_POOL_TXN_HISTORY.shape[0]

    fetched_txns = new_txn_count - old_txn_count

    if fetched_txns > 0:
        print("** Fetched", fetched_txns, "new transactions" if fetched_txns > 1 else "new transaction")
        df_to_csv(DF_POOL_TXN_HISTORY, txn_export_file_name, None, ',')
    else:
        print(f"** We already have the most up-to-date data")
    
    DF_POOL_TXN_HISTORY = DF_POOL_TXN_HISTORY[column_names]

    DF_POOL_TXN_HISTORY['blockNumber'] = DF_POOL_TXN_HISTORY['blockNumber'].apply(int)
    DF_POOL_TXN_HISTORY['timeStamp'] = DF_POOL_TXN_HISTORY['timeStamp'].apply(int)
    DF_POOL_TXN_HISTORY['value'] = DF_POOL_TXN_HISTORY['value'].apply(int)
    
    return DF_POOL_TXN_HISTORY


def fetch_token_txns( target_token, temp_settings ):
    if not target_token: return

    target_token = checkAddress(target_token.strip())

    # Max. number of txns in a single API call
    batch_size = 10000

    # ------------------------------------------------------------------

    txn_export_file_name = f"{target_token}.csv"
    txn_export_csv = find_file(txn_export_file_name)

    column_names = ["blockNumber", "timeStamp", "from", "to", "value"]


    if txn_export_csv:
        DF_POOL_TXN_HISTORY = pd.read_csv(txn_export_csv)
    else:
        DF_POOL_TXN_HISTORY = pd.DataFrame(columns=column_names)
    
    old_txn_count = len(DF_POOL_TXN_HISTORY)

    DF_POOL_TXN_HISTORY['blockNumber'] = DF_POOL_TXN_HISTORY['blockNumber'].apply(int)
    DF_POOL_TXN_HISTORY['timeStamp'] = DF_POOL_TXN_HISTORY['timeStamp'].apply(int)
    DF_POOL_TXN_HISTORY['value'] = DF_POOL_TXN_HISTORY['value'].apply(int)


    if len(DF_POOL_TXN_HISTORY) > 0:
        START_BLOCK_NUMBER = DF_POOL_TXN_HISTORY.iloc[-1]["blockNumber"]
    else:
        START_BLOCK_NUMBER = 0
    
    print("* Checking transactions")

    LIST_DF_PARTIAL_TXNS = [DF_POOL_TXN_HISTORY]

    if START_BLOCK_NUMBER <= temp_settings["SNAPSHOT_BLOCK_NUMBER"]:
        print("* Fetching new transactions")
    
    while START_BLOCK_NUMBER <= temp_settings["SNAPSHOT_BLOCK_NUMBER"]:

        txn_list = getTokenTxnList(None, target_token, temp_settings, START_BLOCK_NUMBER)

        if (txn_list is None) or (len(txn_list) == 0): break

        DF_PARTIAL_TXNS = pd.DataFrame(txn_list, columns=column_names)

        DF_PARTIAL_TXNS['blockNumber'] = DF_PARTIAL_TXNS['blockNumber'].apply(int)
        DF_PARTIAL_TXNS['timeStamp'] = DF_PARTIAL_TXNS['timeStamp'].apply(int)
        DF_PARTIAL_TXNS['value'] = DF_PARTIAL_TXNS['value'].apply(int)

        LIST_DF_PARTIAL_TXNS.append(DF_PARTIAL_TXNS)

        print("*** Received", len(txn_list), "transactions", "              ", end='\r')

        if len(txn_list) < batch_size: break

        START_BLOCK_NUMBER = int(DF_PARTIAL_TXNS.iloc[-1]["blockNumber"])

        sleep(temp_settings["API_CALL_DELAY"])
    
    print()

    if  len(LIST_DF_PARTIAL_TXNS) > 1:
        DF_POOL_TXN_HISTORY = pd.concat(LIST_DF_PARTIAL_TXNS)
        # DF_POOL_TXN_HISTORY = DF_POOL_TXN_HISTORY.astype(str)

        # DF_POOL_TXN_HISTORY["from"] = DF_POOL_TXN_HISTORY["from"].str.lower()
        # DF_POOL_TXN_HISTORY["to"] = DF_POOL_TXN_HISTORY["to"].str.lower()

        DF_POOL_TXN_HISTORY = DF_POOL_TXN_HISTORY.drop_duplicates(subset=column_names, keep="first")

        # DF_POOL_TXN_HISTORY.to_csv(txn_export_file_name, index=False, sep = ',')
        df_to_csv(DF_POOL_TXN_HISTORY, txn_export_file_name, None, ',')
    
    new_txn_count = len(DF_POOL_TXN_HISTORY)
    fetched_txns = new_txn_count - old_txn_count
    
    if fetched_txns > 0:
        print("** Fetched", fetched_txns, "new transactions")
    else:
        print(f"** We already have the most up-to-date data")

    return DF_POOL_TXN_HISTORY

def getTokenTxnList(ofThisWallet, forThisToken, temp_settings, startBlock, endBlock):
    # start and end block results are both included in the API responses

    if ofThisWallet is not None: ofThisWallet = checkAddress(ofThisWallet)

    if not forThisToken: return None
    
    forThisToken = checkAddress(forThisToken)
    
    sleep(temp_settings["API_CALL_DELAY"])

    module = "account"
    action = "tokentx"
    contractaddress = forThisToken
    startblock = startBlock
    endblock = endBlock
    sort = "asc"
    
    chainid = temp_settings["CHAIN_ID"]
    apikey = temp_settings["API_KEY"]
    
    params = {
        "chainid": chainid,
        "module": module,
        "action": action,
        "contractaddress": contractaddress,
        "startblock": startblock,
        "endblock": endblock,
        "sort": sort,
        "apikey": apikey,
    }

    if ofThisWallet is not None: params["address"] = ofThisWallet

    result = make_http_request(temp_settings["API_URL"], target_key="result", parameters=params, headers=None)

    if not result: return None

    return result["data"]


def query_pool(pool, temp_settings):
    pool_name, pool_contract, pool_multiplier = pool

    sleep(temp_settings["API_CALL_DELAY"])
    
    pool_contract = checkAddress(pool_contract)

    if pool_contract is None:
        print()
        print(f"! Error: pool contract address is empty - pool: {pool}")
        print()
    
    sleep(temp_settings["API_CALL_DELAY"])

    pool_contract_owner = checkAddress(getContractOwner(pool_contract, temp_settings))
    
    if pool_contract_owner is None:
        print()
        print(f"! Error: pool contract owner is empty - pool: {pool}")
        print()

    return pool_name, pool_contract, pool_multiplier, pool_contract_owner


def fetch_kyc_data(kyc_settings, kyc_export_filename):
    api_url = kyc_settings["API_URL"]
    api_key = kyc_settings["API_KEY"]
    client_ID = kyc_settings["CLIENT_ID"]

    if api_url is None: return
    if api_key is None: return
    if client_ID is None: return
    if kyc_export_filename is None: return

    print()
    print("* Checking KYC data")

    kyc_export_file = find_file(kyc_export_filename)

    depreciation_period_in_hours = 12
    kyc_data_is_old = download_file_again(kyc_export_file, depreciation_period_in_hours)

    if kyc_data_is_old:
        print(f"** Last KYC data download was more than {depreciation_period_in_hours} hours ago, need to download again")
        print("** Fetching KYC data from provider")
    else:
        print(f"** Last KYC data download was less than {depreciation_period_in_hours} hours ago, no need to download again")
        return

    df_kyc = pd.DataFrame(index=None)

    batchSize = 20
    skippedRecords = 0

    while True:
        try:
            userBatch = getRecords(client_ID, api_url, api_key, "", batchSize, skippedRecords)
            if not userBatch or userBatch is None: break
        except:
            break

        df_userBatch = pd.DataFrame(userBatch, index=None)
        
        df_kyc = pd.concat([df_kyc, df_userBatch], ignore_index=True)
        skippedRecords += len(df_userBatch)

        print("** Fetched", skippedRecords, "records", end='\r')
    
    df_kyc.fillna('', inplace=True)

    if "identities" in df_kyc.columns:
        necessary_kyc_columns = ['refId', 'wallet', 'status', 'recordId', 'blockPassID', 'inreviewDate', 'waitingDate', 'approvedDate']
        
        df_kyc['wallet'] = df_kyc["identities"].apply(lambda x: x.get('crypto_address_eth').get("value"))
    else:
        necessary_kyc_columns = ['refId', 'status', 'recordId', 'blockPassID', 'inreviewDate', 'waitingDate', 'approvedDate']
    
    df_kyc = df_kyc[necessary_kyc_columns]
    
    df_to_csv(df_kyc, f"Raw_{kyc_export_filename}", '', ',')

    df_kyc['status'] = df_kyc['status'].str.lower().str.strip()
    df_kyc["refId"] = df_kyc["refId"].apply(checkAddress)

    if "wallet" in df_kyc.columns:
        df_kyc["wallet"] = df_kyc["wallet"].apply(checkAddress)

    print()

    # ------------------------------

    # df_kyc["approvedDate"] = pd.to_datetime(df_kyc["approvedDate"])
    # df_kyc["approvedDate"] = df_kyc["approvedDate"].dt.strftime("%d.%m.%Y %H:%M")

    # df_kyc["inreviewDate"] = pd.to_datetime(df_kyc["inreviewDate"])
    # df_kyc["inreviewDate"] = df_kyc["inreviewDate"].dt.strftime("%d.%m.%Y %H:%M")

    # df_kyc["waitingDate"] = pd.to_datetime(df_kyc["waitingDate"])
    # df_kyc["waitingDate"] = df_kyc["waitingDate"].dt.strftime("%d.%m.%Y %H:%M")
    
    df_kyc = df_kyc.set_index(keys="refId")

    print("** Saving KYC data")

    df_to_csv(df_kyc, kyc_export_filename, 'refId', ',')
    print("*** Saved as:", kyc_export_filename)


def call_backend_api(api_url, api_key):

    headers = {
        "x-seedify-api-header": api_key,
        'Origin': "https://seedify.fund",
        # 'Origin': "https://stage.develophub.network",
    }

    result = make_http_request(api_url, target_key=None, parameters=None, headers=headers)

    if not result: return None

    return result["data"]

def fetch_registration_data(project_id, api_url, api_key):
    if api_url is None: return pd.DataFrame(), None

    registered_wallets = None
    project_name = None  

    full_api_url = f"{api_url}/igo/{project_id}/interest/export?type=json"

    json_registered = call_backend_api(full_api_url, api_key)

    registered_wallets = json_registered["data"]

    if "idoName" in json_registered.keys():
        project_name = json_registered["idoName"]

    # project_id = json_registered["igoId"]

    df_registered = pd.DataFrame(registered_wallets)

    if df_registered.empty: return pd.DataFrame()
    
    df_registered["primaryWallet"] = df_registered["primaryWallet"].apply(checkAddress)
    # df_registered["delegatedWallet"] = df_registered["delegatedWallet"].apply(checkAddress)
    
    unique_wallets = df_registered['primaryWallet'] \
                         .replace('', pd.NA) \
                         .dropna() \
                         .drop_duplicates() \
                         .reset_index(drop=True)
                        #  .str.lower().str.strip() \
    
    df_registered_unique = pd.DataFrame(unique_wallets, columns=['primaryWallet'])

    # Set primaryWallet as index
    df_registered_unique.set_index('primaryWallet', inplace=True)

    return df_registered_unique, project_name

def fetch_wallet_delegation_data(api_url, api_key):
    if api_url is None: return pd.DataFrame()

    full_api_url = f"{api_url}/user/export?type=json"

    json_wallet_delegation = call_backend_api(full_api_url, api_key)
    wallet_delegation = json_wallet_delegation["data"]

    df_wallet_delegation = pd.DataFrame(wallet_delegation)

    if df_wallet_delegation.empty: return pd.DataFrame()

    df_wallet_delegation.dropna(subset=['delegatedWallet'], inplace=True)
    df_wallet_delegation.dropna(subset=['primaryWallet'], inplace=True)

    df_wallet_delegation["primaryWallet"] = df_wallet_delegation["primaryWallet"].apply(checkAddress)
    df_wallet_delegation["delegatedWallet"] = df_wallet_delegation["delegatedWallet"].apply(checkAddress)

    df_wallet_delegation.drop_duplicates(subset=['primaryWallet'], keep='first', inplace=True)

    df_wallet_delegation.set_index('primaryWallet', inplace=True)

    return df_wallet_delegation


def getRecords(CLIENT_ID_, API_URL_, API_KEY_, status_="", batchSize_=20, skip_=0):

    apiURL = f"{API_URL_}/{CLIENT_ID_}/applicants/{status_}"
    # apiURL = f"{API_URL_}/{CLIENT_ID_}/refId/{wallet}"}

    header = {"Authorization":API_KEY_, "cache-control":"no-cache"}
    params = {"limit":batchSize_, "skip":skip_}

    result = make_http_request(apiURL, target_key="data", parameters=params, headers=header)
    
    if result is None or result == "" or result == [] or result == [""]: return None

    result_data = result["data"]["records"]

    return result_data


def setRPC(RPC_LIST, LAST_RPC_INDEX):
    CURRENT_RPC_INDEX = LAST_RPC_INDEX

    while True:
        CURRENT_RPC_INDEX = (CURRENT_RPC_INDEX + 1) % len(RPC_LIST)
        CURRENT_RPC = RPC_LIST[CURRENT_RPC_INDEX]

        web3 = web3Connection(CURRENT_RPC)

        if web3 is None: continue

        return web3, CURRENT_RPC_INDEX
    

def createContractInstance(web3, contract_address, contract_abi):
    contract_instance = web3.eth.contract(address=contract_address, abi=contract_abi)
    return contract_instance


def delay_retry(error_count, delay):
    if error_count > 0:
        print(f"Retrying in {delay / 1000} seconds.")
        sleep(delay / 1000)
    else:
        print("Max attemps to retry reached, aborting")


def notify_backend(target_url, snapshot_api_key, timestamp):

    session = createRequestSession()
    session.keep_alive = 5

    retries_left = 60
    critical_error_retries_left = 3
    default_delay_retries_ms = 60000
    
    headers = {
        "api-key": snapshot_api_key,
        'Origin': "backend-internal",
    }

    success = False
    while retries_left > 0 and critical_error_retries_left > 0 and not success:
        try:
            response = session.post(f"{target_url}/{timestamp}", params={}, headers=headers, timeout=60)
            
            response.raise_for_status()
            status = response.status_code

            if status in [200, 202]:
                data = response.json()
                if status == 200 and data["status"] == "DONE":
                    success = True
                else:
                    if status == 202:
                        print("Request to save the snapshot successfully created, waiting for it to be processed")
                    elif data["status"] == "IN_PROGRESS":
                        print(f"Waiting for the snapshot to finish processing: {data['msg']}")
                    
                    suggested_delay_ms = data["checkAgainMs"] if data["checkAgainMs"] != None else default_delay_retries_ms
                    retries_left -= 1
                    delay_retry(retries_left, suggested_delay_ms)
            else:
                print(f"Error while saving snapshot information, response code: {status}")
                data = response.json()
                if data["msg"] != None:
                    print(f"Error details: {data['msg']}")

                critical_error_retries_left -= 1
                delay_retry(critical_error_retries_left, default_delay_retries_ms)
            
        except requests.exceptions.RequestException as ex:
            print(f"\nAn exception of type {type(ex).__name__} occurred: {ex}")

            if response is not None:
                print(f"Request response: {response.content}")
            else:
                print("No response received.")


            critical_error_retries_left -= 1
            delay_retry(critical_error_retries_left, default_delay_retries_ms)
    
    session.close()
    
    return success