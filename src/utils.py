# -*- coding: UTF-8 -*-

import sys
import json
import argparse

from os import remove, path, getcwd, chdir, makedirs, name as osname, system
from datetime import datetime, timezone, timedelta
from time import sleep, time
from decimal import Decimal
from glob import glob
from web3 import Web3
from sys import exit

import numpy as np
import pandas as pd


def clear(): system('cls' if osname == 'nt' else 'clear'); print()

def ask_for_token_name(token_list):
    token_name = pickOneFromTheList("Select token", token_list)
    return token_name

def ask_for_date():
    snapshot_preference = pickOneFromTheList("Select snapshot time", [ "Default (last available 1 pm UTC)", "Custom" ])

    if snapshot_preference == "Custom":
        while True:
            try:
                print()
                user_input = input("Enter preferred date without hour/minute (dd.mm.YYYY): ")
                snapshot_datetime = str_to_date(user_input, "%d.%m.%Y")
                
                break
            except ValueError:
                print("Please enter a valid date. Try again.")
        
    else:
        snapshot_datetime = current_datetime_in_utc()
    
    return snapshot_datetime

def pickOneFromTheList(header, item_list):
    list_length = len(item_list)

    if list_length == 0:
        return None, None

    # item_list = natsorted(item_list, alg=ns.IGNORECASE)

    hyphens = "-" * (len(header) + 4)

    while True:
        print()
        print(hyphens)
        print(f"| {header} |")
        print(hyphens)

        for item_index, item in enumerate(item_list):
            line = f"{item_index + 1}) {item}"
            print(line)

        print()

        user_input = input(f"Enter a number between 1-{list_length} (type 'e' or 'q' to exit): ")

        if user_input in ("e", "q", "exit", "quit"):
            print()
            print("Terminating...")
            sleep(0.5)
            exit()

        if not user_input.isdigit():
            wrongInput()
            continue

        user_input = int(user_input)

        if user_input <= 0 or user_input > list_length:
            wrongInput()
        else:
            return item_list[user_input - 1]

def wrongInput():
    print()
    print("This is not a valid number, please try again."), sleep(1.5)


def initialize(config_filename):
    main_dir = getCurrentDir()

    settings = load_json(config_filename)

    output_dir_name = settings["OUTPUT_DIR"]

    if not output_dir_name:
        output_dir_name = "Snapshots"
    
    data_dir_name = settings["DATA_DIR"]
    
    if not data_dir_name:
        data_dir_name = "Data"

    output_dir = createDir(main_dir, output_dir_name)
    data_dir = createDir(main_dir, data_dir_name)

    return settings, main_dir, output_dir, data_dir


def set_snapshot_timestamps(snapshot_datetime, daily_epoch_diff, ssp_period=0):
    print()
    print("* Setting timestamps for snapshots")

    if ssp_period < 1:
        ssp_period = 1

    snapshot_timestamp = date_to_timestamp(snapshot_datetime)

    end_timestamp = snapshot_timestamp
    start_timestamp = end_timestamp - ( ( ssp_period - 1 ) * daily_epoch_diff)
    
    snapshot_timestamps = np.linspace(start_timestamp, end_timestamp, ssp_period, dtype=np.int64)

    return snapshot_timestamps


def initialize_token(data_dir, all_tokens_dict, token_name, network):
    print("* Setting token details")

    token = all_tokens_dict[token_name]
    token_details = token[network]
    token_contract = token_details["contract"]
    lp_contract = token_details["lp_contract"]

    stakes = token_details["stake"]
    farms = token_details["farm"]

    token_dir_name = f"{token_name}_{network}"

    token_dir = createDir(data_dir, token_dir_name)

    return token_dir, token_contract, lp_contract, stakes, farms


def download_file_again(file_path, depreciation_period_in_hours):

    if file_path is None:
        return True
    
    if depreciation_period_in_hours is None:
        depreciation_period_in_hours = 12
    
    depreciation_period_in_seconds = depreciation_period_in_hours * 3600

    if not path.exists(file_path):
        raise FileNotFoundError(f"{file_path} not found.")
    
    # Get last modification time of file
    modification_time = path.getmtime(file_path)

    # # Get creation time of file
    # creation_time = path.getctime(file_path)
    
    # Get current time
    current_time = time()
    passed_time = current_time - modification_time
    
    # Check if the time passed since last modification of file is bigger than the depreciation period
    if passed_time > depreciation_period_in_seconds:
        return True
    else:
        return False

def current_datetime_in_utc():
    return datetime.now(tz=timezone.utc)

def set_hour_to(target_datetime, target_hour, target_minute):
    if not target_datetime: raise ValueError("target_datetime is empty")
    if not target_hour: raise ValueError("target_hour is empty")

    return target_datetime.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0, tzinfo=timezone.utc)

def adjust_snapshot_date(target_snapshot_datetime, preferred_time="13:00"):
    preferred_hour = int(preferred_time.split(":")[0])
    preferred_minute = int(preferred_time.split(":")[1])

    target_snapshot_datetime = set_hour_to(target_snapshot_datetime, preferred_hour, preferred_minute)

    # ------------------------------

    default_snapshot_datetime = current_datetime_in_utc()

    if (default_snapshot_datetime.hour < preferred_hour):
        default_snapshot_datetime -= timedelta(days=1)
    
    default_snapshot_datetime = set_hour_to(default_snapshot_datetime, preferred_hour, preferred_minute)
    
    # ------------------------------

    if (target_snapshot_datetime is None) or (target_snapshot_datetime.date() >= default_snapshot_datetime.date()):
        target_snapshot_datetime = default_snapshot_datetime
    
    return target_snapshot_datetime

def date_to_str(date, format="%d.%m.%Y %H:%M:%S"):
    date_str = date.strftime(format)
    return date_str

def str_to_date(date_str, format="%d.%m.%Y"):
    date = datetime.strptime(date_str, format)
    return date

def date_to_timestamp(date):
    timestamp = date.timestamp()
    return timestamp

def timestamp_to_date(epoch_, format="%d %b %Y %H:%M:%S"):
    date = datetime.fromtimestamp(int(epoch_), tz=timezone.utc).strptime(format)
    return date

def timestamp_to_date_str(epoch_, format="%d %b %Y %H:%M:%S"):
    date = datetime.fromtimestamp(int(epoch_), tz=timezone.utc)
    date_str = date_to_str(date, format)

    return date_str

def setCurrentDir():
    startDir = getCurrentDir()
    currentDir = setActiveDir(startDir)

    return currentDir

def getCurrentDir(): return path.abspath(path.dirname(sys.argv[0]))

def setActiveDir(targetDir=None):
    if not targetDir: targetDir = getcwd()

    chdir(targetDir)
    return targetDir

def createDir(parentDir, targetDir):
    try:
        dirPath = path.join(parentDir, targetDir)
        # if osname == "nt": dirPath = dirPath.replace("\\", "/")

        makedirs(dirPath, exist_ok=True)
        return dirPath
    except OSError as error:
        print(error)

def load_json(file_path):
    print(f"** Checking if {file_path} exists")

    file = find_file(file_path)

    if not file or file is None:
        print(f"! Error: Couldn't locate {file}. Terminating...")
        exit()

    print("** Reading data from:", file_path)

    with open(file_path, 'r') as json_file:
        return json.load(json_file)

def find_file(name):
    try:
        files = glob(str(name))

        if files:
            return files[0]
    except Exception as e:
        print(f"Error finding file: {e}")

    return None

def finalize(network, token_name, CALCULATE_SSP, snapshot_list):
    if snapshot_list is None or len(snapshot_list) == 0:
        return None, None, None
    
    result_df = pd.concat(snapshot_list, axis=1)

    result_df = result_df.fillna(Decimal("0"))

    result_df = result_df.divide(Decimal(1e18))

    stake_columns = []
    farm_columns = []
    lp_columns = []
    ssp_columns = []

    total_stake_and_farm_col_name = f"Total Staked and Farmed {token_name}"
    total_stake_col_name = f"Total Staked {token_name}"
    total_farm_col_name = f"Total Farmed {token_name}"
    total_lp_col_name = "Total LP"

    if CALCULATE_SSP:
        total_ssp_col_name = "Total SSP"
    else:
        total_ssp_col_name = None

    for col in result_df.columns:
        if col.startswith(token_name):
            if "stake" in col.lower():
                stake_columns.append(col)
            elif "farm" in col.lower():
                farm_columns.append(col)
        elif col.startswith("LP"):
            lp_columns.append(col)
        
        if CALCULATE_SSP:
            if col.startswith("SSP"):
                ssp_columns.append(col)
    
    result_df[total_stake_and_farm_col_name] = result_df[stake_columns + farm_columns].sum(axis=1)
    result_df[total_stake_col_name] = result_df[stake_columns].sum(axis=1)
    result_df[total_farm_col_name] = result_df[farm_columns].sum(axis=1)
    result_df[total_lp_col_name] = result_df[lp_columns].sum(axis=1)

    if CALCULATE_SSP:
        result_df[total_ssp_col_name] = result_df[ssp_columns].sum(axis=1)

    # starting columns
    starting_columns = [total_stake_and_farm_col_name, total_stake_col_name, total_farm_col_name, total_lp_col_name]

    if CALCULATE_SSP:
        starting_columns += [total_ssp_col_name]
    
    # full list of columns (starting columns + remaining columns)
    new_order = starting_columns + [col for col in result_df.columns if col not in starting_columns]

    # result_df with new column order
    result_df = result_df[new_order]

    if CALCULATE_SSP:
        columns_to_copy = [total_stake_and_farm_col_name, total_ssp_col_name]
        new_column_names = {total_stake_and_farm_col_name: f"{network} - {token_name}", total_ssp_col_name: f"{network} - SSP"}
    else:
        columns_to_copy = [total_stake_and_farm_col_name]
        new_column_names = {total_stake_and_farm_col_name: f"{network} - {token_name}"}

    return result_df, columns_to_copy, new_column_names

def generate_tier_function(tiers_dict):
    def set_tier(total_token_amount):

        for i in range(len(tiers_dict.keys()) - 1):
            top_limit = tiers_dict[str(i + 1)]["MIN_TOKENS"]

            if total_token_amount < top_limit:
                return i, tiers_dict[str(i)]["POOL_WEIGHT"]
        
        i = list(tiers_dict.keys())[-1]

        return i, tiers_dict[str(i)]["POOL_WEIGHT"]

    return set_tier

def parse_args(tokens_filename):
    snapshot_datetime = None
    project_id = None

    parser = argparse.ArgumentParser(description="""
    Snapshot script that:
    * calculates stake and farm balances
    * calculates tiers
    * calculates seed staking points
    * fetches and sets KYC data
    * creates whitelists for projects by combining IDO registration + wallet delegation
    """, formatter_class=argparse.RawTextHelpFormatter)
    
    # Define arguments
    parser.add_argument("-t", "--token", type=str, help="Sets target token for snapshot (should be an element of token config file)")
    parser.add_argument("-d", "--date", type=str, help="Sets target date for snapshot (in dd.mm.yyyy format)")
    parser.add_argument("-hm", "--hour", type=str, help="Sets target time for snapshot (in hh:mm format)")
    parser.add_argument("-p", "--pools", type=str, help="Sets target pool type for snapshot (values: stake, farm, all [default])")
    parser.add_argument("-id", "--project-id", type=str, help="Combines 'previously created snapshot' + 'registered wallets' + 'delegated wallets' to create project specific whitelist")

    # Parse arguments
    args = parser.parse_args()

    all_tokens_dict = load_json(tokens_filename)
    all_tokens_list = list(all_tokens_dict.keys())

    if len(all_tokens_list) < 1:
        print()
        print(f"! Error: Couldn't find any tokens in {tokens_filename}. Terminating...")
        print()

        exit()
    
    target_tokens_list = None

    if args.token:
        args.token = args.token.upper()

        if args.token in all_tokens_list:
            target_tokens_list = [args.token]

    if target_tokens_list is None:
        target_tokens_list = [all_tokens_list[0]]

    # Convert and verify date
    if args.date:
        if '"' in args.date: args.date = args.date.replace('"', '')
        if "'" in args.date: args.date = args.date.replace("'", "")
        
        try:
            snapshot_datetime = str_to_date(args.date, "%d.%m.%Y")
        except ValueError:
            raise ValueError("Date format is wrong. Correct format: dd.mm.yyyy")
    else:
        snapshot_datetime = current_datetime_in_utc()
    
    if args.hour:
        preferred_time = args.hour
    else:
        preferred_time = "13:00"
    
    snapshot_datetime = adjust_snapshot_date(snapshot_datetime, preferred_time)
    
    if args.pools and args.pools == "stake" or args.pools == "farm":
        target_pools = args.pools
    else:
        target_pools = "all"

    if args.project_id:
        project_id = args.project_id

    return project_id, all_tokens_dict, target_tokens_list, all_tokens_list, snapshot_datetime, target_pools

def deleteFile(targetFile):
    try:
        remove(targetFile)
    except OSError:
        pass

def df_to_csv(df, filename, index_label, seperator=','):
    if index_label is None:
        df.to_csv(filename, index=False, sep=seperator)
    else:
        df.to_csv(filename, index_label=index_label, sep=seperator)

def csv_to_df(filename):
    file = find_file(filename)

    if file is None: return None

    df = pd.read_csv(file)
    return df

def checkAddress(wallet_):
    if not wallet_:
        return None

    wallet_ = str(wallet_).strip()

    if len(wallet_) != 42 or not wallet_.startswith("0x"):
        return None

    try:
        wallet_ = Web3.to_checksum_address(wallet_)
    except Exception:
        return None

    return wallet_

def move_columns_to_head(target_df, target_columns = []):
    if target_df is None: return None
    if target_columns is None: return None

    new_column_order = target_columns + [col for col in target_df.columns if col not in target_columns]

    return target_df[new_column_order]

def end_timer(start_time, row_count=None):
    end_time = time()
    execution_time = round(end_time - start_time, 2)

    per_row = None
    if row_count is not None and row_count > 0:
        per_row = round(execution_time / row_count, 4)

    print()
    print("Execution time:", execution_time, "seconds")

    if per_row is not None:
        print(str(per_row), "seconds/row")
        print(str(row_count) + " rows in total")

    print()