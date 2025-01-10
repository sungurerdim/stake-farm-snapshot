# Author: Sungur Zahid Erdim
# Contact: sungur@seedify.fund, sungurerdim@gmail.com

# -*- coding: UTF-8 -*-

from src.fetch import (
    fetch_pool_txns, epochToBlockNumber, fetch_lp_history, query_pool, 
    find_file, fetch_kyc_data, fetch_registration_data, 
    fetch_wallet_delegation_data, notify_backend
)

from src.utils import (
    clear, end_timer, initialize, initialize_token, finalize, 
    parse_args, setCurrentDir, set_snapshot_timestamps, 
    timestamp_to_date_str, date_to_str, df_to_csv, checkAddress, 
    move_columns_to_head, createDir

)
from src.calculate import (
    calculate, load_kyc_data, process_kyc_data, 
    process_registration_data, process_wallet_delegation_data, 
    process_tiers
)

from src.s3 import s3_download_all, s3_upload_specific_folders

from os import chdir, getenv
from time import time
from sys import exit
from decimal import Decimal

import pandas as pd

def main(tokens_filename, config_filename):
    startTime = time()

    clear()
    setCurrentDir()

    print("* Importing config files")

    project_id, all_tokens_dict, target_tokens_list, all_tokens_list, snapshot_datetime, target_pools  = parse_args(tokens_filename)
    settings, main_dir, output_dir, data_dir = initialize(config_filename)

    print()
    print("* Target token(s):", ", ".join(target_tokens_list))
    print()

    SSP_PERIOD = settings["SSP_PERIOD"]
    CALCULATE_SSP = SSP_PERIOD > 1

    # 24 hours in seconds
    settings["DAILY_EPOCH_DIFF"] = 86400

    snapshot_timestamps = set_snapshot_timestamps(snapshot_datetime, settings["DAILY_EPOCH_DIFF"], SSP_PERIOD)
    snapshot_date_str = date_to_str(snapshot_datetime)

    # ------------------------------

    index_of_snapshot_timestamp = -1 if CALCULATE_SSP else 0
    settings["SNAPSHOT_TIMESTAMP"] = int(snapshot_timestamps[index_of_snapshot_timestamp])

    # ------------------------------

    network_list = []

    for token_name in target_tokens_list:
        networks_of_token = list(all_tokens_dict[token_name].keys())
        if "TIERS" in networks_of_token: networks_of_token.remove("TIERS")

        network_list += networks_of_token
    
    unique_networks_list = sorted(list(set(network_list)), key=str.lower)

    # ------------------------------

    # Load environment variables
    settings["KYC"]["API_URL"] = getenv("KYC_API_URL", None)
    settings["KYC"]["API_KEY"] = getenv("KYC_API_KEY", None)
    settings["KYC"]["CLIENT_ID"] = getenv("KYC_CLIENT_ID", None)
    S3_BUCKET = getenv("S3_BUCKET", None)
    BACKEND_API_URL = getenv("BACKEND_API_URL", None)
    BACKEND_GET_API_KEY = getenv("BACKEND_GET_API_KEY", None)
    BACKEND_POST_API_KEY = getenv("BACKEND_POST_API_KEY", None)
    settings["NETWORK"]["MULTICHAIN_API_KEY"] = getenv("MULTICHAIN_API_KEY", None)

    # ------------------------------

    mandatory_env_vars = {
        "KYC_API_URL": settings["KYC"]["API_URL"],
        "KYC_CLIENT_ID": settings["KYC"]["CLIENT_ID"],
        "KYC_API_KEY": settings["KYC"]["API_KEY"],
        # "S3_BUCKET": S3_BUCKET,
        "BACKEND_API_URL": BACKEND_API_URL,
        "BACKEND_GET_API_KEY": BACKEND_GET_API_KEY,
        # "BACKEND_POST_API_KEY": BACKEND_POST_API_KEY,
    }

    # ------------------------------

    for network in unique_networks_list:
        for r_ind, r in enumerate(settings["NETWORK"][network]["RPC_NODES"]):
            if ".moralis-nodes.com" in r:
                node_key = getenv(f"{network}_MORALIS_RPC_KEY", None)

                if node_key is not None:
                    settings["NETWORK"][network]["RPC_NODES"][r_ind] += node_key
            
            if ".alchemy.com" in r:
                node_key = getenv(f"ALCHEMY_RPC_KEY", None)
                
                if node_key is not None:
                    settings["NETWORK"][network]["RPC_NODES"][r_ind] += node_key
        
        mandatory_env_vars["MULTICHAIN_API_URL"] = settings["NETWORK"]["MULTICHAIN_API_URL"]
        mandatory_env_vars["MULTICHAIN_API_KEY"] = settings["NETWORK"]["MULTICHAIN_API_KEY"]

    # ------------------------------

    print()
    print("* Checking required variables")

    missing_env_vars = [var_name for var_name, value in mandatory_env_vars.items() if value is None]

    # If any required variables are missing, print them and exit the script
    if missing_env_vars:
        print(f"! Error: Missing required variables: {', '.join(missing_env_vars)}")
        exit()
    else:
        print("** All required variables are set, looking fine")
    
    # ------------------------------

    kyc_export_filename = "KYC_EXPORT.csv"

    # ------------------------------

    s3_download_all(S3_BUCKET, main_dir)

    for token_name in target_tokens_list:
        
        # Seed Staking Points (SSP) calculations are only required for SFUND token
        if token_name != "SFUND":
            CALCULATE_SSP = False

        if target_pools == "stake":
            snapshot_filename = f"Raw_{token_name}_Stake_Snapshot.csv"
        elif target_pools == "farm":
            snapshot_filename = f"Raw_{token_name}_Farm_Snapshot.csv"
        else:
            snapshot_filename = f"Raw_{token_name}_Snapshot.csv"

        temp_settings = {
            "SNAPSHOT_TIMESTAMP": settings["SNAPSHOT_TIMESTAMP"],
            "SNAPSHOT_BLOCK_NUMBER": None,
            "SEED_STAKING_START_TIMESTAMP": None,
            "API_URL": None,
            "API_KEY": None,
            "RPC_NODES": None,
            "CUR_RPC_NODE_IDX": None,
            "MAX_RPC_TRY": None,
            "DAILY_EPOCH_DIFF": settings["DAILY_EPOCH_DIFF"],
            "API_CALL_DELAY": settings["NETWORK"]["API_CALL_DELAY"],
        }

        TIERS = None

        if "TIERS" in all_tokens_dict[token_name].keys():
            TIERS = all_tokens_dict[token_name]["TIERS"]
        
        if project_id is None:

            # ------------------------------

            df_snapshot = pd.DataFrame()

            # ------------------------------

            for network in unique_networks_list:
                chdir(data_dir)

                # if network not in all_tokens_dict[token_name].keys(): continue

                temp_settings["SNAPSHOT_TIMESTAMP"] = settings["SNAPSHOT_TIMESTAMP"]
                temp_settings["CHAIN_ID"] = settings["NETWORK"][network]["CHAIN_ID"]

                if settings["NETWORK"][network]["CHAIN_ID"] == "":
                    temp_settings["API_URL"] = settings["NETWORK"][network]["API_URL"]
                    temp_settings["API_KEY"] = settings["NETWORK"][network]["API_KEY"]
                else:
                    temp_settings["API_URL"] = settings["NETWORK"]["MULTICHAIN_API_URL"]
                    temp_settings["API_KEY"] = settings["NETWORK"]["MULTICHAIN_API_KEY"]

                temp_settings["RPC_NODES"] = settings["NETWORK"][network]["RPC_NODES"]
                temp_settings["CUR_RPC_NODE_IDX"] = 0
                temp_settings["MAX_RPC_TRY"] = 3

                temp_settings["SNAPSHOT_BLOCK_NUMBER"] = epochToBlockNumber(temp_settings["SNAPSHOT_TIMESTAMP"], temp_settings)

                print()
                print("#"*20)
                print()
                print("* Snapshot Details *")
                print()
                print(f"Token: {token_name} (on {network} chain)")
                print("Date:", snapshot_date_str)
                print("Timestamp:", temp_settings["SNAPSHOT_TIMESTAMP"])
                print("Block:", temp_settings["SNAPSHOT_BLOCK_NUMBER"])

                if CALCULATE_SSP:
                    print()
                    print("* SSP Details *")
                    print()
                    print("Period:", SSP_PERIOD, "days")
                    print("Start Date:", timestamp_to_date_str(snapshot_timestamps[0]))
                    print("End Date:", timestamp_to_date_str(snapshot_timestamps[-1]))

                print()
                print("-"*10)
                print()

                token_dir, token_contract, lp_contract, stakes, farms = initialize_token(data_dir, all_tokens_dict, token_name, network)
                chdir(token_dir)

                token_contract = checkAddress(token_contract)
                lp_contract = checkAddress(lp_contract)

                snapshot_list = []
                df_network_snapshot = None

                print()
                print("* Gathering data")

                DF_LP_HISTORY = None

                exclude_list = settings["EXCLUDE"].copy()
                pool_list = []

                if target_pools == "farm" or target_pools == "all":
                    DF_LP_HISTORY = fetch_lp_history( lp_contract, token_contract, snapshot_timestamps, temp_settings )

                    print("** Collecting info on farm contracts")

                    for pool in farms:
                        pool = query_pool(pool, temp_settings)

                        target_token = lp_contract
                        lp_history = DF_LP_HISTORY

                        pool+= (target_token,)
                        pool+= (lp_history,)
                        
                        pool_list.append(pool)

                        pool_name, pool_contract, pool_multiplier, pool_contract_owner, target_token, lp_history = pool

                        if not pool_contract in exclude_list: exclude_list.append(pool_contract)
                        if not pool_contract_owner in exclude_list: exclude_list.append(pool_contract_owner)
                
                if target_pools == "stake" or target_pools == "all":
                    print("** Collecting info on stake contracts")
                    
                    for pool in stakes:
                        pool = query_pool(pool, temp_settings)

                        target_token = token_contract
                        lp_history = None

                        pool+= (target_token,)
                        pool+= (lp_history,)
                        
                        pool_list.append(pool)

                        pool_name, pool_contract, pool_multiplier, pool_contract_owner, target_token, lp_history = pool
                        
                        if not pool_contract in exclude_list: exclude_list.append(pool_contract)
                        if not pool_contract_owner in exclude_list: exclude_list.append(pool_contract_owner)
                
                if target_pools == "farm" or target_pools == "all":
                    print(f"** Collecting info on possible farm contracts with {token_name} in them")

                    other_tokens = all_tokens_list.copy()
                    other_tokens.remove(token_name)

                    for other_token_name in other_tokens:
                        if not network in all_tokens_dict[other_token_name].keys(): continue

                        other_token_details = all_tokens_dict[other_token_name][network]
                        other_lp_contract = other_token_details["lp_contract"]

                        if other_lp_contract is None or other_lp_contract == '': continue

                        other_token_farms = other_token_details["farm"]

                        target_token = checkAddress(token_contract)
                        other_lp_contract = checkAddress(other_lp_contract)

                        DF_LP_HISTORY_OTHER = None
                        DF_LP_HISTORY_OTHER = fetch_lp_history( other_lp_contract, token_contract, snapshot_timestamps, temp_settings )

                        if DF_LP_HISTORY_OTHER is None: continue

                        for pool in other_token_farms:
                            pool = query_pool(pool, temp_settings)

                            target_token = other_lp_contract
                            lp_history = DF_LP_HISTORY_OTHER

                            pool+= (target_token,)
                            pool+= (lp_history,)

                            pool_list.append(pool)

                            pool_name, pool_contract, pool_multiplier, pool_contract_owner, target_token, lp_history = pool
                                                
                            if not pool_contract in exclude_list: exclude_list.append(pool_contract)
                            if not pool_contract_owner in exclude_list: exclude_list.append(pool_contract_owner)
                
                print()
                print(f"* Processing all pools/contracts")

                for pool in pool_list:
                    pool_name, pool_contract, pool_multiplier, pool_contract_owner, target_token, lp_history = pool

                    print()
                    print("-"*10)
                    print()
                    print("Pool:", pool_name)
                    print("Contract:", pool_contract)
                    print("Contract Owner:", pool_contract_owner)
                    print("Target Token:", target_token)

                    if CALCULATE_SSP:
                        print("SSP Multiplier:", pool_multiplier)

                    print()

                    df_pool_txns = fetch_pool_txns( pool, temp_settings )
                    df_pool_snapshot = calculate( token_name, df_pool_txns, pool, snapshot_timestamps, exclude_list, CALCULATE_SSP, df_lp_history = lp_history )

                    snapshot_list.append(df_pool_snapshot)
                
                df_network_snapshot, columns_to_copy, new_column_names = finalize( network, token_name, CALCULATE_SSP, snapshot_list )
                
                chdir(output_dir)

                if (df_network_snapshot is not None) and (not df_network_snapshot.empty):
                    print()
                    print(f"* Saving {network} snapshot")

                    df_snapshot = df_snapshot.sort_index()

                    if target_pools == "stake":
                        network_snapshot_filename = f"{token_name}_{network}_Stake_Snapshot.csv"
                    elif target_pools == "farm":
                        network_snapshot_filename = f"{token_name}_{network}_Farm_Snapshot.csv"
                    else:
                        network_snapshot_filename = f"{token_name}_{network}_Snapshot.csv"

                    df_to_csv(df_network_snapshot, network_snapshot_filename, 'Wallet', ',')

                    print("** Saved as:", network_snapshot_filename)

                    df_snapshot = pd.concat([ df_snapshot, df_network_snapshot[columns_to_copy] ], axis=1)
                    df_snapshot = df_snapshot.rename(columns=new_column_names)
                    
            # ------------------------------

            df_snapshot = df_snapshot.fillna(Decimal("0"))

            if (df_snapshot is not None) and (not df_snapshot.empty):

                if TIERS is not None:
                    print()
                    print("#"*20)

                    print()
                    print("* Calculating tiers and seed staking points")

                    df_snapshot = process_tiers(df_snapshot, token_name, TIERS, CALCULATE_SSP)
                else:
                    total_tokens_column_name = f"Total {token_name}"
                    df_snapshot[total_tokens_column_name] = df_snapshot[df_snapshot.columns].sum(axis=1).apply(Decimal)

                    df_snapshot = df_snapshot[ [total_tokens_column_name] + [ col for col in df_snapshot.columns if col != total_tokens_column_name ] ]
                
                print()
                print("-"*10)
                print()
                print("* Saving snapshot")

                chdir(output_dir)

                df_to_csv(df_snapshot, snapshot_filename, 'Wallet', ',')

                print("** Saved as:", snapshot_filename)

            # ------------------------------

            chdir(data_dir)

            if token_name == "SFUND":
                fetch_kyc_data(settings["KYC"], kyc_export_filename)

        elif project_id is not None and token_name == "SFUND":

            chdir(output_dir)

            if find_file(snapshot_filename) is None:
                print()
                print(f"! Error: Couldn't locate previously created raw snapshot file -> {snapshot_filename}")
                print()
                print("Terminating...")
                print()

                exit()

            df_snapshot = pd.read_csv(snapshot_filename)
            df_snapshot.set_index('Wallet', inplace=True)

            if (df_snapshot is not None) and (not df_snapshot.empty):

                print()
                print("#"*20)
                print()
                print("Project ID:", project_id)

                # ------------------------------

                print()
                print("* Loading KYC data")

                chdir(data_dir)

                df_kyc = load_kyc_data(kyc_export_filename)

                if not df_kyc.index.empty:
                    df_snapshot = process_kyc_data(df_snapshot, df_kyc)
                
                # ------------------------------

                print()
                print("* Fetching IDO registration data")

                df_registered, project_name = fetch_registration_data(project_id, BACKEND_API_URL, BACKEND_GET_API_KEY)

                if project_name is not None:
                    filename_suffix = project_name
                else:
                    filename_suffix = project_id
                
                project_dir = createDir(output_dir, f"{project_name}_{project_id}")
                chdir(project_dir)
                
                if not df_registered.index.empty:
                    print(f"* Saving IDO registration data ({df_registered.shape[0]} wallets)")

                    reg_filename = f"{filename_suffix}_IDO_Registration_Export.csv"
                    df_to_csv(df_registered, reg_filename, 'Wallet', ',')
                    
                    print(f"** Saved as {reg_filename}")
                    
                    df_snapshot = process_registration_data(df_snapshot, df_registered)

                # ------------------------------

                print()
                print("* Fetching wallet delegation data")

                df_wallet_delegation = fetch_wallet_delegation_data(BACKEND_API_URL, BACKEND_GET_API_KEY)

                if not df_wallet_delegation.index.empty:
                    print(f"* Saving wallet delegation data ({df_wallet_delegation.shape[0]} wallets)")

                    wallet_delegation_filename = f"{filename_suffix}_Wallet_Delegation_Export.csv"
                    df_to_csv(df_wallet_delegation, wallet_delegation_filename, 'Wallet', ',')
                    
                    print(f"** Saved as {wallet_delegation_filename}")

                    df_snapshot = process_wallet_delegation_data(df_snapshot, df_wallet_delegation, df_registered, df_kyc)

                # ------------------------------
                if TIERS is not None:
                    print()
                    print("* Calculating tiers and seed staking points")

                    df_snapshot = process_tiers(df_snapshot, token_name, TIERS, CALCULATE_SSP)

                # ------------------------------

                columns_to_move = []
                
                if not df_registered.index.empty:
                    columns_to_move.append('Registration')

                if not df_kyc.index.empty:
                    columns_to_move.append('KYC')
                

                df_snapshot = move_columns_to_head(df_snapshot, columns_to_move)

                # ------------------------------

                print()
                print("-"*10)

                print()
                print("* Saving combined (kyc + registration + wallet delegation) snapshot")

                combined_snapshot_filename = f"{filename_suffix}_Snapshot.csv"
                df_to_csv(df_snapshot, combined_snapshot_filename, 'Wallet', ',')

                print("** Saved as:", combined_snapshot_filename)


                # ------------------------------

                print()
                print("* Saving whitelist")

                if TIERS is not None:
                    df_snapshot["Tier"] = df_snapshot["Tier"].apply(int)
                    df_whitelist = df_snapshot[ ( df_snapshot["Tier"] > 0 ) & ( df_snapshot["Registration"] == "registered" ) & ( df_snapshot["KYC"] == "approved" )]
                else:
                    df_whitelist = df_snapshot[ ( df_snapshot["Registration"] == "registered" ) & ( df_snapshot["KYC"] == "approved" )]

                whitelist_filename = f"{filename_suffix}_Whitelist.csv"
                df_to_csv(df_whitelist, whitelist_filename, 'Wallet', ',')

                print("** Saved as:", whitelist_filename)

                # ------------------------------

                if TIERS is not None:
                    print()
                    print("* Creating tier files")

                    del TIERS["0"]
                    
                    for tier_num, df_tier in df_whitelist.groupby('Tier'):
                        tier_filename = f"Tier{tier_num}_{filename_suffix}.csv"
                        df_tier_index = df_tier.index.str.lower()
                        df_tier_index.to_frame(index=False).to_csv(tier_filename, header=False, index=False)
                        print(f"** Saved Tier {tier_num} wallets to {tier_filename}")
    
    # ------------------------------

    s3_upload_specific_folders(S3_BUCKET, [data_dir, output_dir], "")

    if BACKEND_POST_API_KEY is not None:
        print()
        print("* Triggering snapshot data update on backend")

        result = notify_backend(f"{BACKEND_API_URL}/snapshot", BACKEND_POST_API_KEY, settings["SNAPSHOT_TIMESTAMP"])

        if result == True:
            print("** Snapshot data update is complete")
        else:
            print("! An error happened while updating the information in seedify-backend, check console logs and seedify-backend API logs for more details.")

    print()
    print("-"*10)

    print()
    print("* Snapshot process is complete")

    end_timer(startTime)

if __name__ == "__main__":
    config_filename = "config.json"
    tokens_filename = "tokens.json"

    main(tokens_filename, config_filename)
