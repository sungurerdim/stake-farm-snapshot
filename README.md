
# Snapshot Automation Script

This project automates the process of:
- creating/taking blockchain snapshots,
- calculating wallet/user tiers
- calculating wallet/user seed staking points (SSP)
- fetching/cleaning/integrating KYC data,
- fetching/cleaning/integrating IDO registration data,
- fetching/cleaning/integrating wallet delegation data
- creating IDO specific whitelists (list of wallets that can join an IDO)

Script:
- is a complete rewrite of previous version, to integrate KYC, IDO registration and wallet delegation features.
- uses a list of RPC archive nodes and iterates over them when necessary, to make sure all the required data is fetched completely.
- uses S3 bucket for data storage

-----

_**Raw Snapshot:**  `Basic snapshot of wallet stake and farm balances on the smart contracts listed in **tokens.json**`_

_**IDO Whitelist**  `Combination of "Raw snapshot" + "KYC Data" + "IDO Registration Data" + "Wallet Delegation Data"`_

-----

## Table of Contents
1. [Project Structure](#project-structure)
2. [Setup](#setup)
3. [Usage](#usage)
4. [Environment Variables](#environment-variables)
5. [Configuration Files](#configuration-files)
6. [Main Files Overview](#main-files-overview)
7. [Additional Notes](#additional-notes)
8. [Process Flow (Detailed)](#process-flow-detailed)

## Project Structure
```
- config.json
- main.py
- requirements.txt
- tokens.json
- /src
    - calculate.py
    - fetch.py
    - s3.py
    - utils.py
```

## Setup

### Install Dependencies
Ensure Python 3.7+ is installed, then install the dependencies using:
```bash
pip install -r requirements.txt
```

### AWS S3 Configuration
- Ensure you have AWS credentials configured on the environment (script doesn't use them directly).
- Ensure you have an S3 bucket and it is set as S3_BUCKET environment variable.

## Usage

Available arguments/parameters:

- `-t` , `--token`        ->    Optional - Sets target token for snapshot (should be an element of token config file). Defaults to the first token in **tokens.json**
- `-d` , `--date`         ->    Optional - Sets target date for snapshot (in dd.mm.yyyy format). Defaults to today
- `-hm` , `--hour`         ->    Optional - Sets target time for snapshot (in HH:MM format). Defaults to the last available 1 pm UTC
- `-p`, `--pools`         ->    Optional - Sets target pool type for snapshot (values: stake, farm, all [default])
- `-id` , `--project-id`  ->    Required for whitelist creation - Combines 'raw snapshot' + 'kyc' + 'registered wallets' + 'delegated wallets' to create project specific whitelist


Raw snapshot creation:

    ```bash
    python main.py -t TOKEN_NAME -d dd.mm.yyyy -hm HH:MM
    ```

Example:

    ```bash
    python main.py -t SFUND -d 01.01.2025 -hm 09:45
    ```


IDO whitelist creation:

    ```bash
    python main.py -id project_id
    ```

Example:

    ```bash
    python main.py -id 66ec19cd8c97e60c5dc3aaad
    ```

## Environment Variables
This project uses environment variables for secure key handling. No API keys or sensitive information should be stored in configuration files.

- **`${network}_MORALIS_RPC_KEY`**: The Moralis key for the RPC node (if it will be used).
- **`ALCHEMY_RPC_KEY`**: The Alchemy key for the RPC node (if it will be used).

Other required environment variables:
- **`S3_BUCKET`**: The name of the S3 bucket used for uploading and downloading snapshots.
- **`KYC_API_URL`**: The API URL for Blockpass API calls.
- **`KYC_API_KEY`**: The API key for Blockpass API calls.
- **`KYC_CLIENT_ID`**: The Client ID for Blockpass API calls.
- **`BACKEND_API_URL`**: The base URL for interacting with the backend system for registration and wallet delegation data.
- **`BACKEND_GET_API_KEY`**: The API key used for GET requests to the backend.
- **`BACKEND_POST_API_KEY`**: The API key used for POST requests to the backend.
- **`MULTICHAIN_API_KEY`**: The Etherscan multi-chain API key.

Ensure all these variables are set in your environment before running the script.

## Configuration Files

### `config.json`
This file stores critical settings and values for the snapshot process, such as:

- **Snapshot Settings**:
  - `SSP_PERIOD`: Number of days in the seed staking points calculation period.
  
- **Directories**:
  - `OUTPUT_DIR`: The directory for storing snapshots.
  - `DATA_DIR`: The directory for storing input data.

- **KYC Settings**:
  - `CLIENT_ID`: The Blockpass client ID for fetching KYC data.
  - `API_URL`: The URL for the Blockpass KYC API.

- **Network Settings**:
  - `API_CALL_DELAY`: Number of seconds to wait between API calls (can be increased to not reach rate limits).
  - `MULTICHAIN_API_URL`: Etherscan multi-chain API URL.
  
  For each network (e.g., BNB, ETH, ARB):
  - `CHAIN_ID`: Etherscan Chain ID of specified network
  - `RPC_NODES`: Contains list of RPC archive nodes

- **EXCLUDE**:
  A list of wallet addresses that should be excluded from the snapshot process. These are typically blacklisted or internal addresses that should not be considered in calculations.

### `tokens.json`
This file defines the tokens and staking/farming pools used in the snapshot process:

- **Token Information**:
  - Type (`bep20`, `erc20`), decimals, and contract addresses for tokens on different networks (e.g., BNB, ETH, ARB).

- **Staking/Farming Pools**:
  - Contract addresses for staking and farming pools, along with associated data.

## Main Files Overview

### `main.py`
This is the main entry point. It orchestrates the fetching, processing, and uploading of snapshot data.

### `calculate.py`
Handles filtering, processing of transaction data, and calculating tiers.

### `fetch.py`
Fetches data from blockchain nodes and APIs, including KYC and transaction data.

### `s3.py`
Manages the downloading and uploading of snapshot files from AWS S3.

### `utils.py`
Utility functions for general file operations, data formatting, and user input handling.

## Additional Notes
- Ensure
  - all necessary API keys are set as environment variables
      - S3_BUCKET
      - BACKEND_API_URL
      - BACKEND_GET_API_KEY
      - BACKEND_POST_API_KEY
      - KYC_API_URL
      - KYC_API_KEY
      - KYC_CLIENT_ID
      - MULTICHAIN_API_KEY
  - tokens.json is present and has at least 1 token with all the required fields
  - config.json is present and has all the required fields
- The script will rotate through multiple RPC nodes in case one fails, ensuring smooth operation.

## Process Flow (Detailed)
1 - Save starting time to startTime
2 - Load configurations from ${config_filename} and ${tokens_filename}
3 - Parse arguments

    token (target token to check for balances in the smart contracts):
    - if "-t / --token" is empty or not used, set the first token in the ${tokens_filename} as the target token
    - if "-t / --token" is used
      - if specified token is an element of ${tokens_filename}, set it as the target token
      - if specified token is not an element of ${tokens_filename}, set the first token in the ${tokens_filename} as the target token

    date (target date to take the snapshot, can be any date in the past):
    - if "-d / --date" is empty or not used, set the date as today
    - if "-d / --date" is used
      - if the format of specified date is correct (dd.mm.yyyy), set it as the snapshot date
      - if the format of specified date is not correct (dd.mm.yyyy), display error message and terminate

    time (target hour:minute to take the snapshot):
    - if "-hm / --hour" is empty or not used, set the hour:minute as 1 pm UTC
    - if "-hm / --hour" is used
      - if the format of specified time is correct (HH:MM), set it as the snapshot time
      - if the format of specified time is not correct (HH:MM), display error message and terminate
    
    pools (target hour:minute to take the snapshot):
    - if "-p / --pools" is empty or not used, set it as "all"
    - if "-p / --pools" is used, set pool type as specified

    project id (target IDO to create the whitelist):
    - if "-id / --project-id" is empty or not used, create raw snapshot (that only has staked + farmed token balances, without "IDO registration + KYC + wallet delegation" details)
    - if "-id / --project-id" is used
      - find previously created raw snapshot ("Raw_${token_name}_Snapshot.csv")
      - create refined/processed snapshot by combining "raw snapshot + IDO registration + KYC + wallet delegation" details ("${project_name}_Snapshot.csv")
      - create whitelist (list of wallets that can join an IDO) by filtering refined snapshot with whitelisting rules ("${project_name}_Whitelist.csv")
      - divide whitelist based on tiers ("Tier${tier_no}_${project_name}.csv")

4 - Create a list of timestamps for seed staking points calculation

Seed staking points (SSP) are basically the points you get from staking/farming for the given period of time (default is last 90 days).

Pool multipliers are the elements used to make the pools with longer lock period more attractive to users. These are defined in ${tokens_filename} for each token, network and contract.

5 - Create a unique list of networks from the list of tokens to process for future iterations.
This is required to avoid duplicate tasks when there are multiple tokens with similar/same networks.
6 - Get required keys/details from environment variables and check if they are empty, terminate script if any of them are missing.
7 - Download previously created files from S3 bucket (skips if S3_BUCKET env var is empty or not set)
8 - Iterate over the list of tokens to process
8.1 - create a temporary dictionary of variables to pass to functions
8.2 - get tier details of the token from ${tokens_filename}
8.3 - if project id is empty or not used:
8.3.1 - iterate over unique network list
8.3.1.1 - initialize token
8.3.1.1.1 - get all token details (networks, contracts etc.) from ${tokens_filename}
8.3.1.1.2 - create directory for "${token_name}_${network}"
8.3.1.2 - verify token contract and lp contract addresses
8.3.1.3 - fetch lp history (token amounts for the lp contract for each day of SSP_PERIOD, will be used for SSP calculation of farm pools which use that lp token)
8.3.1.4 - iterate over all stake and farm pools of the target token and add them to the list of pools to process
8.3.1.5 - iterate over all stake and farm pools of other tokens in the ${tokens_filename}, add them to the list of pools to process if their lp token has the target token in it and fetch the lp history for related lp tokens
This is required to calculate all the tokens of wallets in all lp tokens.
Let's say user has staked SFUND in a stake pool and has some lp tokens in the SNFTS farm pool (which has SNFTS and SFUND tokens inside the pair). This step calculates the SFUND in the SNFTS farm and adds it to the total staked+farmed balance.
8.3.2 - Process all the pools found in the previous 2 steps
8.3.2.1 - fetch pool transactions (all the transactions of pool contract, starts from day zero ends at snapshot date/time)
8.3.2.2 - calculate pool balances
8.3.2.2.1 - filter transactions (remove excluded wallets) and get unique list of wallets for future use
8.3.2.2.2 - process transactions and calculate balances based on SSP timestamps and snapshot timestamp
8.3.2.2.3 - if current pool is a farm, convert token balances (which is lp token at first, because farm contracts require users to lock lp tokens) to target token balances
The formula used for this calculation is:
"user's lp token amount" / "total number of lp tokens in circulation" * "total number of target tokens inside all the lp tokens in circulation" = "user's target token balance for that given time"

"user's lp token amount" - calculated by script (fetched from the previously created snapshot timestamps dataframe, which has lp token balances for each day of SSP_PERIOD)
"total number of lp tokens in circulation" - fetched from lp token's smart contract function (totalSupply)
"total number of target tokens inside all the lp tokens in circulation" - fetched from lp token's smart contract function (getReserves) (this returns 2 values, we find the one we need by running token0 and token1 functions of the same smart contract and compare with the target token's contract)

This calculation is being done for each day of SSP_PERIOD.

8.3.2.2.4 - calculate SSP
For a single day/snapshot:
"wallet balance of a wallet" * "pool multiplier" / 100

Let's say user has 1500 SFUND tokens in "30 days stake pool" (with a pool multiplier of 0.5) on BNB chain.
For a single day, user will get 1500 * 0.5 / 100 = 7.5 seed staking points

This gets calculated for each day of SSP_PERIOD and the snapshots show the total SSP for wallets.

8.3.2.2.5 - change column order (move lp amount and SSP columns to first place)

8.3.3 - finalize snapshot file (create total sum columns, change column order, pick the columns to include in combined snapshot)
8.3.4 - save the snapshot as "${token_name}_${network}_Snapshot.csv"
8.3.5 - calculate tiers, if token has a TIERS key in ${tokens_filename}
8.3.5 - fetch KYC data from Blockpass