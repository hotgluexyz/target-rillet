import os
import gluestick as gs  # Hotglue's transformation library
import pandas as pd  # For data manipulation (DataFrames)
import json
from utils import (
    read_mapping,
    get_currency_name,
    country_alpha3_from_currency,
    map_expense_lines,
    get_id_from_snapshot,
    get_service_period,
    map_charge_lines,
    map_reimbursement_items,
    map_vendor_credit_lines,
    map_account_type,
    map_cashback_items,
    prune_unsent_hash_entries,
    get_tags_dict,
    get_line_item_custom_fields,
    get_subsidiaries_reference_ids,
    read_connector_config,
    get_bank_account_id,
    get_payment_bill_id,
    get_payment_vendor_credit_id,
    get_account_subsidiary_id,
)


# Directory Configuration
ROOT_DIR = os.environ.get("ROOT_DIR", ".")
INPUT_DIR = os.path.join(ROOT_DIR, "sync-output")
SNAPSHOT_DIR = os.path.join(ROOT_DIR, "snapshots")
OUTPUT_DIR = os.path.join(ROOT_DIR, "etl-output")
TARGET_CATALOG_PATH = os.path.join(ROOT_DIR, "target-catalog.json")

# Environment Configuration
TENANT_ID = os.environ.get("TENANT")
FLOW_ID = os.environ.get("FLOW", "KQhsz4w0j")
JOB_TYPE = os.environ.get("JOB_TYPE", "read")
API_KEY = os.environ.get("API_KEY")
ENV_ID = os.environ.get("ENV_ID")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# read tenant config
tenant_config_path = os.path.join(SNAPSHOT_DIR, "tenant-config.json")

if os.path.exists(tenant_config_path):
    with open(tenant_config_path, "r") as f:
        tenant_config = json.load(f)
else:
    tenant_config = {}

# read default mapping
mapping = read_mapping()

# read connector config
connector_config = read_connector_config()

# read data
input = gs.Reader()

if JOB_TYPE == "write":

    # make sure subsidiaries are not empty and get list of subsidiary names
    subsidiaries = input.get("subsidiaries", catalog_types=True)

    if subsidiaries is None or subsidiaries.empty:
        raise ValueError("No subsidiaries found")
    
    # map currencies from subsidiaries
    if subsidiaries is not None and not subsidiaries.empty:
        currencies = subsidiaries[["currency"]].drop_duplicates().reset_index(drop=True)
        currencies_mapping = mapping["write"]["currencies"]
        # get name for currencies from isocode
        currencies["name"] = currencies["currency"].apply(lambda x: get_currency_name(x))
        currencies = [gs.map_fields(currency, currencies_mapping) for _, currency in currencies.iterrows()]

        # convert back to dataframe
        currencies = pd.DataFrame(currencies)

        # get ids
        currencies = get_id_from_snapshot(currencies, "Currencies")

        # send currencies to airbase
        gs.to_singer(currencies, "Currencies", output_dir=OUTPUT_DIR)
    
    # map subsidiaries to v2 model
    if subsidiaries is not None and not subsidiaries.empty:
        # Country alpha-3 from subsidiary currency (business defaults in utils.country_alpha3_from_currency).
        subsidiaries["country"] = subsidiaries["currency"].apply(country_alpha3_from_currency)
        subsidiaries_mapping = mapping["write"]["subsidiaries"]
        subsidiaries = [gs.map_fields(subsidiary, subsidiaries_mapping) for _, subsidiary in subsidiaries.iterrows()]

        # convert back to dataframe
        subsidiaries = pd.DataFrame(subsidiaries)

        # get ids
        subsidiaries = get_id_from_snapshot(subsidiaries, "Subsidiaries")

        # snapshot subsidiaries
        gs.snapshot_records(subsidiaries[["subsidiaryNumber", "externalId"]], "rillet_subsidiaries", SNAPSHOT_DIR, pk="subsidiaryNumber")

        # send subsidiaries to airbase
        gs.to_singer(subsidiaries, "Subsidiaries", output_dir=OUTPUT_DIR, allow_objects=True)

    subsidiaries_list, subsidiary_reference_ids = get_subsidiaries_reference_ids(subsidiaries["name"].to_list())

    # send accounts
    accounts = input.get("accounts", catalog_types=True)
    bank_accounts = input.get("bank_accounts", catalog_types=True)
    FALLBACK_CURRENCY = "USD"
    # map accounts to v2 model
    if accounts is not None and not accounts.empty:
        if bank_accounts is not None and not bank_accounts.empty:
            accounts = accounts.merge(bank_accounts[["currency", "subsidiary_id", "account_code"]], left_on="code", right_on="account_code", how="left")
        
        accounts["currency"] = accounts["currency"].fillna(FALLBACK_CURRENCY) if "currency" in accounts.columns else FALLBACK_CURRENCY 
        accounts["subsidiaries"] = accounts.apply(lambda x: get_account_subsidiary_id(x, subsidiaries_list), axis=1)
        # filter only accounts of type accounts_payable,expense, bank or credit_card
        accounts["type"] = accounts.apply(lambda x: map_account_type(x), axis=1)
        accounts = accounts[accounts["type"].notna()]

        # map accounts to v2 model
        accounts_mapping = mapping["write"]["accounts"]
        accounts = [gs.map_fields(account, accounts_mapping) for _, account in accounts.iterrows()]

        # convert back to dataframe
        accounts = pd.DataFrame(accounts)

        # get ids
        accounts = get_id_from_snapshot(accounts, "Accounts")

        # send accounts to airbase
        gs.to_singer(accounts, "Accounts", output_dir=OUTPUT_DIR, allow_objects=True)

    # send tags
    fields = input.get("fields", catalog_types=True)
    if fields is not None and not fields.empty:
        fields["values"] = fields["values"].apply(gs.parse_objs)
        fields = gs.explode_json_to_rows(fields, "values")
        # filter out deactivated values
        fields = fields[~fields["values.deactivated"]]
        # keep and rename relevant columns
        fields = fields[["id","name", "values.id", "values.name"]]
        fields = fields.rename(columns={"id": "field_id","name": "type_label", "values.id": "value_id", "values.name": "name"})
        # build externalId from record id and values id
        fields["externalId"] = fields.apply(lambda x: f"{x['field_id']}___{x['value_id']}", axis=1)

        fields["subsidiary_reference_ids"] = [subsidiary_reference_ids for _ in range(len(fields))]
        fields_mapping = mapping["write"]["tags"]
        fields = [gs.map_fields(tag, fields_mapping) for _, tag in fields.iterrows()]
        fields = pd.DataFrame(fields)
        gs.to_singer(fields, "tags", output_dir=OUTPUT_DIR, allow_objects=True)

    # send vendors
    vendors = input.get("vendors", catalog_types=True)

    # map vendors to v2 model
    if vendors is not None and not vendors.empty:
        vendors["subsidiaries"] = [subsidiaries_list for _ in range(len(vendors))]

        # map vendors to v2 model
        vendors_mapping = mapping["write"]["vendors"]
        vendors = [gs.map_fields(vendor, vendors_mapping) for _, vendor in vendors.iterrows()]

        # convert back to dataframe
        vendors = pd.DataFrame(vendors)

        # get ids
        vendors = get_id_from_snapshot(vendors, "Vendors")

        # send vendors to airbase
        gs.to_singer(vendors, "Vendors", output_dir=OUTPUT_DIR, allow_objects=True)

else:
    mapping = mapping.get("read", {})

    vendors = input.get("vendors", catalog_types=True)
    if vendors is not None and not vendors.empty:
        vendors_mapping = mapping["vendors"]
        vendors = [gs.map_fields(vendor, vendors_mapping) for _, vendor in vendors.iterrows()]
        vendors = pd.DataFrame(vendors)

        # retry vendors that were hashed but never successfully synced to airbase
        prune_unsent_hash_entries("vendors", SNAPSHOT_DIR, FLOW_ID, pk="externalId")
        # only keep vendors that are new or have been updated
        vendors = gs.drop_redundant(vendors, "vendors", SNAPSHOT_DIR, pk=["externalId"])
        # export vendors to to_singer
        gs.to_singer(vendors, "vendors", output_dir=OUTPUT_DIR, allow_objects=True, recursive_typing=False)

    # map bills from airbase to rillet
    ledger_entries = input.get("ledger_entries", catalog_types=True)
    subsidiaries_snapshot = gs.read_snapshots(f"Subsidiaries_{FLOW_ID}", SNAPSHOT_DIR)
    rillet_subsidiaries = gs.read_snapshots(f"rillet_subsidiaries", SNAPSHOT_DIR)

    if ledger_entries is not None and not ledger_entries.empty and subsidiaries_snapshot is not None and not subsidiaries_snapshot.empty:

        if rillet_subsidiaries is not None and not rillet_subsidiaries.empty:
            subsidiary_ids = rillet_subsidiaries.set_index("subsidiaryNumber")["externalId"].to_dict()
        else:
            subsidiary_ids = subsidiaries_snapshot.set_index("RemoteId")["InputId"].to_dict()
    
        # parse nested fields
        parse_cols = ["vendor", "currency", "subsidiary", "expense_list", "ap_account"]
        explode_cols = ["vendor", "currency", "subsidiary", "ap_account"]
        for col in parse_cols:
            ledger_entries[col] = ledger_entries[col].apply(lambda x: gs.parse_objs(x))
            if col in explode_cols:
                ledger_entries = gs.explode_json_to_cols(ledger_entries, col)
        
        # keep only ledger entries that belong to subsidiaries imported from rillet
        ledger_entries = ledger_entries[ledger_entries["subsidiary.erp_reference_id"].isin(subsidiary_ids.keys())]
        ledger_entries["subsidiary_id"] = ledger_entries["subsidiary.erp_reference_id"].map(subsidiary_ids)

    # create bank accounts for subsidiaries
    # get the account from a gpa_load entry
    if ledger_entries is not None and not ledger_entries.empty:
        load_entry_types = ["gpa_load", "bank_payment_load", "bank_payment_unload"]
        load_entries = ledger_entries[ledger_entries["type"].isin(load_entry_types)| ledger_entries["bank_account"].notna()] 
        created_bank_accounts = gs.read_snapshots(f"bank-accounts_{FLOW_ID}", SNAPSHOT_DIR)
        if load_entries.empty and (created_bank_accounts is None or created_bank_accounts.empty):
            raise ValueError("Not able to create bank accounts for subsidiaries, clearing account was not found in any transaction.")
        # dedup by subsidiary_id
        load_entries = load_entries.drop_duplicates(subset=["subsidiary_id", "currency.iso_code"])
        
        # get the first record
        mapped_bank_accounts = []
        for i, load_entry in load_entries.iterrows():
            load_record = gs.parse_objs(load_entries.iloc[0])
            if load_record["type"] in load_entry_types:
                account = load_record["expense_list"][0].get("account", {}).get("erp_reference_id")
            else:
                account = gs.parse_objs(load_record["bank_account"]).get("erp_reference_id")
            currency = load_record.get("currency.iso_code")
            # create bank account for subsidiary
            bank_account = {
                "account_code": account,
                "subsidiary_id": load_record["subsidiary_id"],
                "name": f"Airbase {currency}",
                "currency": currency,
                "bank_name": "Airbase",
                "transactions_start_date": connector_config.get("financial_transactions_start_date", "2000-01-01"),
                "externalId": f"{load_record['subsidiary.erp_reference_id']}--{currency}",
            }
            mapped_bank_accounts.append(bank_account)

        df = pd.DataFrame(mapped_bank_accounts)
        gs.to_singer(df, "bank-accounts", output_dir=OUTPUT_DIR, allow_objects=True)
    
    # only proceed if there are ledger entries to process
    if ledger_entries is not None and not ledger_entries.empty:
        # add external references
        ledger_entries["external_references"] = ledger_entries.apply(lambda x: [{"type": "Airbase-Hotglue", "id": x["airbase_id"], "url": x["airbase_url"]}], axis=1)

        # get service period from entry data
        ledger_entries["service_period"] = ledger_entries.apply(lambda x: get_service_period(x), axis=1)

        # format datetimes
        ledger_entries["due_date"] = ledger_entries["entry_date"] + pd.DateOffset(days=30)

        datetime_columns = ["due_date", "entry_date", "payment_entry_date"]
        for column in datetime_columns:
            ledger_entries[column] = ledger_entries[column].dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        
        if "vendor.erp_reference_id" in ledger_entries.columns and "vendor.airbase_id" in ledger_entries.columns:
            # not that vendors are also sent Airbase -> Rillet, we might create a vendor but that vendor won't have an erp_reference_id
            # so we need to use the airbase_id, the target will create the vendor and use the airbase_id to get the Rillet new created vendor id
            empty = ledger_entries["vendor.erp_reference_id"].isna() | ledger_entries["vendor.erp_reference_id"].astype(str).str.strip().eq("")
            ledger_entries["vendor.erp_reference_id"] = ledger_entries["vendor.erp_reference_id"].where(~empty, ledger_entries["vendor.airbase_id"])

        # get fields in Rillet format
        tags_dict = get_tags_dict()
        ledger_entries["expense_list"] = ledger_entries.apply(lambda x: get_line_item_custom_fields(tags_dict, x["expense_list"]), axis=1)

        # filter all transactions by financial_transactions_start_date
        if ledger_entries is not None and not ledger_entries.empty:
            ledger_entries = ledger_entries[ledger_entries["entry_date"] >= connector_config.get("financial_transactions_start_date", "2000-01-01")]

        # map bills 
        bills = ledger_entries[ledger_entries["type"].isin(["bill"])]
        if bills is not None and not bills.empty:        
            bills_mapping = mapping["bills"]
            expenses_mapping = mapping["bill_expenses"]
            bills["expenses"] = bills.apply(lambda x: map_expense_lines(x, expenses_mapping), axis=1)
            bills["custom_fields"] = bills.apply(lambda x: [{"name":"external_references", "value": x["external_references"]}], axis=1)
            bills = [gs.map_fields(bill, bills_mapping) for _, bill in bills.iterrows()]

            # convert back to dataframe
            bills = pd.DataFrame(bills)

            # get ids
            bills = get_id_from_snapshot(bills, "Bills")

            # send bills to rillet
            gs.to_singer(bills, "Bills", output_dir=OUTPUT_DIR, allow_objects=True, recursive_typing=False)
            del bills

        # map charges
        charges = ledger_entries[ledger_entries["type"].isin(["virtual_card_payment", "physical_card_payment", "refund"])]
        if charges is not None and not charges.empty:
            # get credit card gl account per subsidiary from tenant config
            credit_card_gl_accounts = [mapping for mapping in tenant_config.get("hotglue_mapping", {}).get("mapping", {}).get("airbase", []) if mapping.get("source") == "subsidiary_to_credit_card_gl"]
            if credit_card_gl_accounts:
                credit_card_mapping = {v:k for k,v in credit_card_gl_accounts[0].get("fields", {}).items()}
            else:
                raise ValueError("No credit card accounts were mapped for any subsidiary")

            charges_mapping = mapping["charges"]
            charge_lines_mapping = mapping["charge_lines"]
            charges["credit_card_account_code"] = charges["subsidiary.erp_reference_id"].map(credit_card_mapping)
            charges["items"] = charges.apply(lambda x: map_charge_lines(x, charge_lines_mapping), axis=1)
            charges = [gs.map_fields(charge, charges_mapping) for _, charge in charges.iterrows()]
            charges = pd.DataFrame(charges)
            charges = get_id_from_snapshot(charges, "Charges")
            gs.to_singer(charges, "charges", output_dir=OUTPUT_DIR, allow_objects=True, recursive_typing=False)
            del charges
        

        # map reimbursements
        reimbursements = ledger_entries[ledger_entries["type"].isin(["reimbursement"])]
        if reimbursements is not None and not reimbursements.empty:
            # for reimbursements the vendor returned is not a GL vendor is just a vendor, we need to create these in Rillet
            reimbursement_vendors = reimbursements[["vendor.name", "vendor.airbase_id"]].rename(columns={"vendor.name": "name", "vendor.airbase_id": "externalId"}).drop_duplicates()
            gs.to_singer(reimbursement_vendors, "vendors", output_dir=OUTPUT_DIR, allow_objects=True, recursive_typing=False)

            # export reimbursements to rillet
            reimbursements_mapping = mapping["reimbursements"]
            reimbursements["items"] = reimbursements.apply(lambda x: map_reimbursement_items(x, mapping["reimbursement_items"]), axis=1)
            reimbursements = [gs.map_fields(reimbursement, reimbursements_mapping) for _, reimbursement in reimbursements.iterrows()]
            reimbursements = pd.DataFrame(reimbursements)
            reimbursements = get_id_from_snapshot(reimbursements, "Reimbursements")
            gs.to_singer(reimbursements, "reimbursements", output_dir=OUTPUT_DIR, allow_objects=True, recursive_typing=False)
            del reimbursements
        
        # map cashbacks
        cashbacks = ledger_entries[ledger_entries["type"].isin(["cashback"])]
        if cashbacks is not None and not cashbacks.empty:
            cashbacks_mapping = mapping["cashbacks"]
            cashbacks["items"] = cashbacks.apply(lambda x: map_cashback_items(x, mapping["cashback_items"]), axis=1)
            cashbacks = [gs.map_fields(cashback, cashbacks_mapping) for _, cashback in cashbacks.iterrows()]
            cashbacks = pd.DataFrame(cashbacks)
            cashbacks = get_id_from_snapshot(cashbacks, "Cashbacks")
            gs.to_singer(cashbacks, "JournalEntries", output_dir=OUTPUT_DIR, allow_objects=True, recursive_typing=False)
            del cashbacks


        vendor_credits = ledger_entries[ledger_entries["type"].isin(["vendor_credit"])]
        if vendor_credits is not None and not vendor_credits.empty:
            vendor_credit_vendors = vendor_credits[["vendor.name", "vendor.airbase_id"]].rename(columns={"vendor.name": "name", "vendor.airbase_id": "externalId"}).drop_duplicates()
            gs.to_singer(vendor_credit_vendors, "vendors", output_dir=OUTPUT_DIR, allow_objects=True, recursive_typing=False)

            vendor_credits_mapping = mapping["vendor_credits"]
            vendor_credit_lines_mapping = mapping["vendor_credit_lines"]
            vendor_credits["line_items"] = vendor_credits.apply(
                lambda x: map_vendor_credit_lines(x, vendor_credit_lines_mapping), axis=1
            )
            vendor_credits = [gs.map_fields(vendor_credit, vendor_credits_mapping) for _, vendor_credit in vendor_credits.iterrows()]
            vendor_credits = pd.DataFrame(vendor_credits)
            gs.to_singer(vendor_credits, "vendor-credits", output_dir=OUTPUT_DIR, allow_objects=True, recursive_typing=False)
            del vendor_credits

        airbase_fees = ledger_entries[ledger_entries["type"].isin(["airbase_fees"])]
        if airbase_fees is not None and not airbase_fees.empty:
            airbase_fees = airbase_fees.assign(externalId=airbase_fees["airbase_id"])
            gs.to_singer(airbase_fees[["externalId"]], "airbase-fees", output_dir=OUTPUT_DIR, allow_objects=True, recursive_typing=False)
        

        # send gpa_loads, gpa_unloads, bank_payment_loads and bank_payment_unloads to rillet
        bank_transactions = ledger_entries[ledger_entries["type"].isin(["gpa_load", "gpa_unload", "bank_payment_load", "bank_payment_unload", "virtual_card_payment", "physical_card_payment", "refund", "reimbursement"])]
        # filter only bank_transactions that are equal or after financial_transactions_start_date
        if bank_transactions is not None and not bank_transactions.empty:    
            # parse amounts to be negative or positive
            POSITIVE_TRANSACTION_TYPES = ["gpa_load", "bank_payment_load", "refund"]
            bank_transactions["total_amount"] = bank_transactions["total_amount"].astype(float)
            bank_transactions["total_amount"] = bank_transactions.apply(lambda trans: trans["total_amount"] * -1 if trans["type"] not in POSITIVE_TRANSACTION_TYPES else trans["total_amount"], axis=1)
            # mark all as pending False, airbase transactions are always processed
            bank_transactions["pending"] = False
            bank_transactions["vendor.name"] = bank_transactions["vendor.name"].fillna("")
            bank_transactions["notes"] = bank_transactions["notes"].fillna("")
            bank_transactions["bank_account_id"] = bank_transactions.apply(lambda x: get_bank_account_id(x), axis=1)
            bank_transactions_mapping = mapping["bank_transactions"]
            bank_transactions = [gs.map_fields(bank_transaction, bank_transactions_mapping) for _, bank_transaction in bank_transactions.iterrows()]
            bank_transactions = pd.DataFrame(bank_transactions)
            gs.to_singer(bank_transactions, "bank-transactions", output_dir=OUTPUT_DIR, allow_objects=True, recursive_typing=False)
            del bank_transactions

        # map bill payments
        vendor_credit_payments = None
        bill_payments = ledger_entries[ledger_entries["type"].isin(["vendor_payment"])]
        if bill_payments is not None and not bill_payments.empty:
            # explode bank_account
            bill_payments["bank_account"] = bill_payments.apply(lambda x: gs.parse_objs(x["bank_account"]), axis=1)
            bill_payments = gs.explode_json_to_cols(bill_payments, "bank_account")
            # get bill_id from applied_to (target will replace airbase_id with rillet id using snapshots)
            bill_payments["bill_id"] = bill_payments["applied_to"].apply(lambda x: get_payment_bill_id(x))
            # get vendor_credit_id from applied_to if exists(target will replace airbase_id with rillet id using snapshots)
            bill_payments["vendor_credit_id"] = bill_payments["applied_to"].apply(lambda x: get_payment_vendor_credit_id(x))

            # filter vendor_credit_payments
            vendor_credit_payments = bill_payments[bill_payments["bill_id"].notna() &bill_payments["vendor_credit_id"].notna()]
            # filter bill_payments
            bill_payments = bill_payments[bill_payments["bill_id"].notna() & bill_payments["vendor_credit_id"].isna()]

        if vendor_credit_payments is not None and not vendor_credit_payments.empty:
            vendor_credit_payments_mapping = mapping["vendor_credit_payments"]
            vendor_credit_payments = [gs.map_fields(vendor_credit_payment, vendor_credit_payments_mapping) for _, vendor_credit_payment in vendor_credit_payments.iterrows()]
            vendor_credit_payments = pd.DataFrame(vendor_credit_payments)
            gs.to_singer(vendor_credit_payments, "vendor_credit_payments", output_dir=OUTPUT_DIR, allow_objects=True)
            del vendor_credit_payments
        
        if bill_payments is not None and not bill_payments.empty:
            bill_payments_mapping = mapping["bill_payments"]
            bill_payments = [gs.map_fields(bill_payment, bill_payments_mapping) for _, bill_payment in bill_payments.iterrows()]
            bill_payments = pd.DataFrame(bill_payments)
            gs.to_singer(bill_payments, "bill_payments", output_dir=OUTPUT_DIR, allow_objects=True)
            del bill_payments
    