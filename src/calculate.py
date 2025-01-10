import pandas as pd
import numpy as np

from decimal import Decimal, InvalidOperation

from src.utils import find_file, generate_tier_function, move_columns_to_head


def filter_txns(df_pool_txns, exclude_list):
    if (df_pool_txns is None) or (len(df_pool_txns) == 0):
        print()
        print("** Error: df_pool_txns is empty")
        print()
        
        return None, None
    
    print("* Filtering transactions")
    
    unique_wallets = np.setdiff1d(np.unique(df_pool_txns[['from', 'to']].values), exclude_list)

    from_copy = df_pool_txns[df_pool_txns['from'].isin(unique_wallets)][["timeStamp", "from", "value"]].rename(columns={"from": "wallet"}).copy()
    to_copy = df_pool_txns[df_pool_txns['to'].isin(unique_wallets)][["timeStamp", "to", "value"]].rename(columns={"to": "wallet"}).copy()

    to_copy["value"] = -1 * to_copy["value"]

    merged = pd.concat([to_copy,from_copy]).sort_values("timeStamp", ascending=True)
    
    df_pool_txns_filtered = merged.set_index(keys="wallet")

    if df_pool_txns_filtered is None:
        print()
        print("** Error: df_pool_txns_filtered is empty")
        print(df_pool_txns.info())
        print()
    
    if unique_wallets is None:
        print()
        print("** Error: unique_wallets is empty")
        print(df_pool_txns.info())
        print()

    return df_pool_txns_filtered, unique_wallets


def process_txns(df_pool_txns_filtered, unique_wallets, snapshot_timestamps):
    
    if df_pool_txns_filtered is None:
        return None
    
    number_of_txns_to_process = len(df_pool_txns_filtered)

    if number_of_txns_to_process == 0:
        return None
    
    print("* Processing", number_of_txns_to_process, "transactions for", len(unique_wallets), "unique wallets")

    df_pool_snapshot = pd.DataFrame(int(0), index=unique_wallets, columns=snapshot_timestamps, dtype=np.object_)

    prev_stmp = 0

    def positive_cumulative_sum(iterable):
        total = int(0)
        for item in iterable:
            total = max(0, total + int(item))
        
        return total

    def positive_cumulative_sum_2(x):
        initial = df_pool_snapshot.loc[x.name, prev_stmp]
        iterable = x.values

        total = int(initial)
        for item in iterable:
            total = max(0, total + int(item))

        return total

    for i in range(len(snapshot_timestamps)):
        
        cur_stamp = snapshot_timestamps[i]

        timestamp_condition = (prev_stmp < df_pool_txns_filtered["timeStamp"]) & (df_pool_txns_filtered["timeStamp"] <= cur_stamp)

        if i == 0:
            merged_cumulative_sum = df_pool_txns_filtered[timestamp_condition].groupby("wallet")["value"].apply(positive_cumulative_sum)
            
        else:
            merged_cumulative_sum = df_pool_txns_filtered[timestamp_condition].groupby("wallet")["value"].apply(positive_cumulative_sum_2)
            df_pool_snapshot.loc[df_pool_snapshot.index.difference(merged_cumulative_sum.index), cur_stamp] = df_pool_snapshot.loc[df_pool_snapshot.index.difference(merged_cumulative_sum.index), prev_stmp]

        df_pool_snapshot.loc[merged_cumulative_sum.index, cur_stamp] = merged_cumulative_sum.values

        prev_stmp = cur_stamp

    return df_pool_snapshot


def calculate_balance(df_pool_txns, unique_wallets, snapshot_timestamp):
    
    if df_pool_txns is None or len(df_pool_txns) == 0:
        return
    
    print("* Processing transactions")

    balance_column_name = "Balance"

    df_pool_snapshot = pd.DataFrame(int(0), index=unique_wallets, columns=[balance_column_name], dtype=np.object_)

    timestamp_condition = (df_pool_txns["timeStamp"] <= snapshot_timestamp)
    merged_cumulative_sum = df_pool_txns[timestamp_condition].sort_values("timeStamp", ascending=True).groupby("wallet")["value"].sum().apply(Decimal)

    df_pool_snapshot.loc[merged_cumulative_sum.index, balance_column_name] = merged_cumulative_sum.values

    print("** Process complete")

    return df_pool_snapshot


def calculate(token_name, df_pool_txns, pool, snapshot_timestamps, exclude_list, CALCULATE_SSP, df_lp_history=None):
    
    pool_name, pool_contract, pool_multiplier, pool_contract_owner, target_token, lp_history = pool

    df_pool_txns_filtered, unique_wallets = filter_txns(df_pool_txns, exclude_list)

    df_pool_snapshot = process_txns(df_pool_txns_filtered, unique_wallets, snapshot_timestamps)

    final_snapshot_timestamp = df_pool_snapshot.columns.values[-1]
    
    if (df_lp_history is not None) and (not df_lp_history.empty):
        print("* Converting LP token amounts to SFUND token amounts")

        copy_column = df_pool_snapshot[final_snapshot_timestamp].copy()

        df_lp_history["ratio"] = df_lp_history["tokenAmount"].apply(int) / df_lp_history["lpAmount"].apply(int)
        df_lp_history["ratio"] = df_lp_history["ratio"].apply(Decimal)

        df_pool_snapshot = df_pool_snapshot.apply(lambda row: row * df_lp_history["ratio"].T.apply(Decimal), axis=1)

    total_column_name = f"{token_name} ({pool_name})"
    df_pool_snapshot.rename(columns={final_snapshot_timestamp: total_column_name}, inplace=True)

    def convert_to_decimal(x):
        try:
            return Decimal(str(x))
        except (ValueError, TypeError, InvalidOperation):
            return None
        
    # Convert to Decimal using DataFrame.map
    df_pool_snapshot_decimal = df_pool_snapshot.map(convert_to_decimal).dropna()

    # Ensure pool_multiplier is Decimal
    pool_multiplier = Decimal(str(pool_multiplier))

    # Add new SSP column
    if CALCULATE_SSP:
        ssp_column_name = f"SSP ({pool_name})"
        df_pool_snapshot[ssp_column_name] = (
            df_pool_snapshot_decimal.mul(pool_multiplier).div(Decimal("100")).sum(axis=1)
        )

    column_order = [ total_column_name ]
    
    if (df_lp_history is not None) and (not df_lp_history.empty):
        print("* Adding LP column to results dataframe")

        LP_column_name = f"LP ({pool_name})"
        df_pool_snapshot[LP_column_name] = copy_column

        column_order += [ LP_column_name ]

    if CALCULATE_SSP:
        column_order += [ ssp_column_name ]
    
    return df_pool_snapshot.loc[:, column_order]


def load_kyc_data(kyc_filename):
    kyc_file = find_file(kyc_filename)
        
    if not kyc_file: return pd.DataFrame()

    df_kyc = pd.read_csv(kyc_file)

    df_kyc.set_index("refId", inplace=True)

    print(f"** Loaded {df_kyc.shape[0]} records from {kyc_file}")

    return df_kyc

def select_row(group):
    # pick the row with "approved" status, if there is one
    approved_rows = group[group['KYC'] == 'approved']
    if not approved_rows.empty:
        return approved_rows.iloc[0]
    
    # pick the first found row, if there is no "approved" one
    return group.iloc[0]

def process_kyc_data(df_snapshot, df_kyc):
    print("* Processing KYC data")

    df_kyc = df_kyc.rename(columns={'status': 'KYC'})
    df_kyc.dropna(subset=['KYC'], inplace=True)

    df_kyc = df_kyc.sort_index()

    # Assign wallet as refID, if a wallet doesn't have any refID set to it
    def assign_wallet_as_refid(row):
        if pd.isna(row.name) and row['wallet'] not in df_kyc.index:
            return row['wallet']
        return row.name
    
    df_kyc.index = df_kyc.apply(assign_wallet_as_refid, axis=1)

    # First, group by refID to pick the correct row
    df_kyc = df_kyc.groupby(df_kyc.index, group_keys=False).apply(select_row)

    df_kyc = df_kyc[df_kyc.index.notna()]

    # Now, group by wallet to resolve duplicates in wallet column
    df_kyc = df_kyc.groupby('wallet', group_keys=False).apply(select_row)

    missing_wallets_kyc = df_kyc.index.difference(df_snapshot.index)
    df_missing_wallets_kyc = pd.DataFrame(Decimal("0"), index=missing_wallets_kyc, columns=df_snapshot.columns)

    df_snapshot = pd.concat([df_snapshot, df_missing_wallets_kyc])

    df_snapshot['KYC'] = 'no_data'

    df_snapshot = df_snapshot.fillna(Decimal("0"))

    df_snapshot.loc[df_snapshot.index.isin(df_kyc.index), 'KYC'] = df_kyc.loc[df_kyc.index, 'KYC']

    print(f"** Processed {df_kyc.shape[0]} records")

    return df_snapshot


def process_registration_data(df_snapshot, df_registered):
    if df_snapshot is None: return None
    if df_registered is None: return df_snapshot

    print("* Processing IDO registration data")

    missing_wallets_registration = df_registered.index.difference(df_snapshot.index)
    df_missing_wallets_registration = pd.DataFrame(Decimal("0"), index=missing_wallets_registration, columns=df_snapshot.columns)

    df_snapshot = pd.concat([df_snapshot, df_missing_wallets_registration])
    
    df_snapshot['Registration'] = 'not_registered'

    df_snapshot.loc[df_snapshot.index.isin(df_missing_wallets_registration.index), 'KYC'] = 'no_data'
    df_snapshot = df_snapshot.fillna(Decimal("0"))

    df_snapshot.loc[df_snapshot.index.isin(df_registered.index), 'Registration'] = 'registered'

    print("** Process complete")
    
    return df_snapshot


def process_wallet_delegation_data(df_snapshot, df_wallet_delegation, df_registered, df_kyc):
    if df_snapshot is None: return None
    if df_wallet_delegation is None: return df_snapshot

    print("* Processing wallet delegation data")

    missing_wallets_wd_primary = df_wallet_delegation.index.difference(df_snapshot.index)
    df_missing_wallets_wd_primary = pd.DataFrame(Decimal("0"), index=missing_wallets_wd_primary, columns=df_snapshot.columns)

    df_snapshot = pd.concat([df_snapshot, df_missing_wallets_wd_primary])

    missing_wallets_wd_delegated = df_wallet_delegation.loc[~df_wallet_delegation["delegatedWallet"].isin(df_snapshot.index), "delegatedWallet"]
    df_missing_wallets_wd_delegated = pd.DataFrame(Decimal("0"), index=missing_wallets_wd_delegated, columns=df_snapshot.columns)

    df_snapshot = pd.concat([df_snapshot, df_missing_wallets_wd_delegated])

    df_snapshot.loc[df_snapshot.index.isin(df_missing_wallets_wd_primary.index), 'KYC'] = 'no_data'
    df_snapshot.loc[df_snapshot.index.isin(df_missing_wallets_wd_primary.index), 'Registration'] = 'not_registered'

    df_snapshot.loc[df_snapshot.index.isin(df_missing_wallets_wd_delegated.index), 'KYC'] = 'no_data'
    df_snapshot.loc[df_snapshot.index.isin(df_missing_wallets_wd_delegated.index), 'Registration'] = 'not_registered'

    df_snapshot = df_snapshot.fillna(Decimal("0"))

    # 1. Iterate through each primary/delegated pair
    for primary, row in df_wallet_delegation.iterrows():
        delegated = row['delegatedWallet']
        
        if pd.notna(delegated):  # Skip if no delegated wallet
            # Check and set KYC
            if df_kyc is not None:
                if (df_snapshot.loc[[primary, delegated], 'KYC'] == 'approved').any():
                    df_snapshot.loc[[primary, delegated], 'KYC'] = 'approved'
                else:
                    df_snapshot.loc[[primary, delegated], 'KYC'] = df_snapshot.loc[delegated, 'KYC']
            
            # Check and set Registration
            if df_registered is not None:
                if (df_snapshot.loc[[primary, delegated], 'Registration'] == 'registered').any():
                    df_snapshot.loc[[primary, delegated], 'Registration'] = 'registered'
                else:
                    df_snapshot.loc[[primary, delegated], 'Registration'] = df_snapshot.loc[delegated, 'Registration']

            # Replace primary wallet with delegated wallet
            df_snapshot = df_snapshot.rename(index={primary: delegated})
    
    df_snapshot[df_snapshot.columns.difference(['KYC', 'Registration'])] = df_snapshot[df_snapshot.columns.difference(['KYC', 'Registration'])].apply(lambda x: x.astype(float))

    # 2. Combine rows with the same wallet index, summing numeric columns
    df_snapshot = df_snapshot.groupby(df_snapshot.index).agg({
        **{col: 'sum' for col in df_snapshot.select_dtypes(include='number').columns},
        **{col: 'first' for col in df_snapshot.select_dtypes(exclude='number').columns}
    })

    # ------------------------------

    print("** Process complete")
    
    return df_snapshot


def process_tiers(df_snapshot, token_name, TIER_DETAILS, CALCULATE_SSP):
    tier_column_name = "Tier"
    total_tokens_column_name = f"Total {token_name}"
    total_ssp_column_name = "Total SSP"

    pool_weight_column_name = "Pool Weight"
    ssp_percent_column_name = "SSP %"

    # ------------------------------

    column_order = []

    if TIER_DETAILS is not None:
        column_order += [ tier_column_name, pool_weight_column_name ]

    column_order += [ total_tokens_column_name ]

    if CALCULATE_SSP:
        column_order += [ total_ssp_column_name, ssp_percent_column_name ]
    
    # ------------------------------

    df_snapshot.drop(columns=column_order, errors='ignore', inplace=True)

    # ------------------------------

    token_columns = list(filter(lambda col: token_name in col, df_snapshot.columns))
    df_snapshot[total_tokens_column_name] = df_snapshot[token_columns].sum(axis=1).apply(Decimal)

    # ------------------------------

    if CALCULATE_SSP:
        ssp_columns = list(filter(lambda col: "SSP" in col, df_snapshot.columns))
        df_snapshot[total_ssp_column_name] = df_snapshot[ssp_columns].sum(axis=1).apply(Decimal)
    
        total_ssp = df_snapshot[total_ssp_column_name].sum(axis=0)
        df_snapshot[ssp_percent_column_name] = ( df_snapshot[total_ssp_column_name] / total_ssp ) * 100

    if TIER_DETAILS is not None:
        tier_function = generate_tier_function(TIER_DETAILS)
        df_snapshot[tier_column_name], df_snapshot[pool_weight_column_name] = zip(*df_snapshot[total_tokens_column_name].apply(tier_function))

    df_snapshot = move_columns_to_head(df_snapshot, column_order)

    return df_snapshot