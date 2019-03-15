import oci
import os
import gzip
import shutil
import pandas as pd
import datetime
import requests
from requests.auth import HTTPBasicAuth
import json
from termcolor import colored

# This script downloads all of the usage reports for a tenancy (specified in the config file) and adds rates from
# the metering API (https://docs.oracle.com/en/cloud/get-started/subscriptions-cloud/meter/Use_Cases.html).
#
# Pre-requisites:
# 1. Find your cloud_account_ID. You can extract it from the URL for your My Services Dashboard
#       (Example: cacct-db1a7ac1123b4e62be56ae7e9abe8b06)
# 2. Find your IDCS ID. You get extract it from the URL for the sign in page when you pick oraclecloudidenity as the provider.
#       (Example: idcs-386789c7a70b4078a89ca8280b2bda6a)
# 3. Create an OCI SDK config file + private key (https://docs.cloud.oracle.com/iaas/Content/API/Concepts/sdkconfig.htm)
# 4. Put your IDCS username and password in the u.txt and p.txt files
# 5. Create an IAM policy to endorse users in your tenancy to read usage reports from the OCI tenancy
#        Example policy:
#           define tenancy usage-report as ocid1.tenancy.oc1..aaaaaaaaned4fkpkisbwjlr56u7cj63lf3wffbilvqknstgtvzub7vhqkggq
#           endorse group <group_name> to read objects in tenancy usage-report
#
# Note - the only values you need to change is <group name>. No not change the OCID in the first statement

usage_report_namespace = 'bling'

# Cloud account data needed to get rate card
cloud_account_id = 'cacct-db1a7ac1123b4e62be56ae7e9abe8b06'
idcs_id = 'idcs-386789c7a70b4078a89ca8280b2bda6a'
idcs_username = open('u.txt', 'r').read()
idcs_password = open('p.txt', 'r').read()
oldest_file_to_process = datetime.datetime.strptime('2019-02-02', '%Y-%m-%d')

# Update these values
destination_path = 'downloaded_reports'
destination_path_cost = 'downloaded_reports_cost'
config_path = 'config/config'
progress_made_file = 'progress'
resource_lookup_path = 'ResourceLookup.csv'

# Make a directory to receive reports
if not os.path.exists(destination_path):
    os.mkdir(destination_path)

if not os.path.exists(destination_path_cost):
    os.mkdir(destination_path_cost)

# Load progress marker. This is the filename of the last file that was downloaded and processed successfully
last_report_processed = open(progress_made_file, 'r').read()

# Get the list of usage reports. Reports must be downloaded from the home region.
config = oci.config.from_file(config_path,'DEFAULT')
usage_report_bucket = config['tenancy']
object_storage = oci.object_storage.ObjectStorageClient(config)
report_bucket_objects = object_storage.list_objects(usage_report_namespace, usage_report_bucket,\
                                                    fields='name,size,timeCreated',\
                                                    start='reports/usage-csv/' + last_report_processed)

# Create the rate card
rate_card = pd.DataFrame(columns=['Resource', 'UnitPrice', 'Currency', 'PartNumber'])

for o in report_bucket_objects.data.objects:
    object_details = object_storage.get_object(usage_report_namespace, usage_report_bucket, o.name)
    filename = o.name.rsplit('/', 1)[-1]

    if (filename != last_report_processed) and (o.time_created > oldest_file_to_process):
        with open(destination_path + '/' + filename, 'wb') as f:
            for chunk in object_details.data.raw.stream(1024 * 1024, decode_content=False):
                f.write(chunk)

        print('Downloaded ' + o.name + ' (created on ' + str(o.time_created) + ')')

        with gzip.open(destination_path + '/' + filename, 'rb') as f_in:
            with open(destination_path + '/' + filename.replace('.gz', ''), 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

        report_without_cost = pd.read_csv(destination_path + '/' + filename.replace('.gz',''))

        # find the max date in the report to query for rate card details
        min_date_str = report_without_cost['lineItem/intervalUsageStart'].min()
        max_date_str = report_without_cost['lineItem/intervalUsageEnd'].max()
        min_date = datetime.datetime.strptime(max_date_str, "%Y-%m-%dT%H:%MZ").date()
        max_date = datetime.datetime.strptime(max_date_str, "%Y-%m-%dT%H:%MZ").date()

        # download the usage for this day and extract rates
        endpoint = 'https://itra.oraclecloud.com/metering/api/v1/usagecost'
        cacct = cloud_account_id
        custom_headers = {'X-ID-TENANT-NAME': idcs_id}
        start_date = (min_date + datetime.timedelta(days=-1)).strftime('%Y-%m-%d')
        start_time = '00:00:00.000'
        start_date_time = start_date + 'T' + start_time
        end_date = max_date.strftime('%Y-%m-%d')
        end_time = '23:59:59.000'
        end_date_time = end_date + 'T' + end_time
        usage_type = 'DAILY'
        time_zone = 'Europe/London'
        compute_type_enabled = 'Y'
        rollup_level = 'RESOURCE'

        my_url = endpoint + '/' + cacct + '?' + \
                 'startTime=' + start_date_time + '&endTime=' + end_date_time + \
                 '&computeTypeEnabled=' + compute_type_enabled + \
                 '&timeZone=' + time_zone + '&usageType=' + usage_type + '&rollupLevel=' + rollup_level

        response = requests.get(my_url, auth=HTTPBasicAuth(idcs_username, idcs_password),
                                headers=custom_headers)

        json_parsed = json.loads(str(response.text))
        json_data = json_parsed['items']

        for rate in json_data:
            existing_rate = rate_card[rate_card.Resource == rate['resourceName']]
            if existing_rate.shape[0] == 0:
                rate_line = pd.DataFrame([[rate['resourceName'], rate['costs'][0]['unitPrice'], rate['currency'], rate['gsiProductId']]], columns=['Resource', 'UnitPrice', 'Currency', 'PartNumber'])
                rate_card = rate_card.append(rate_line)

        # Load the downloaded file into a dataframe to merge in costs
        report_without_cost = pd.read_csv(destination_path + '/' + filename.replace('.gz', ''))

        # Join CSV data into the resource lookup then into the rate card
        conversion_sheet = pd.read_csv(resource_lookup_path)
        report_with_conversion = pd.merge(left=report_without_cost, right=conversion_sheet, how='left',
                                          left_on='product/resource', right_on='Resource')

        # Check for missing values in the conversion chart - Missing values are likely for new SKUs.
        # Without the conversion, you will be unable to join into rates.
        conversion_check = pd.isna(report_with_conversion['Conversion'])
        missing_conversion = report_with_conversion[conversion_check]

        if missing_conversion.shape[0] > 0:
            print(colored('The following resources are missing conversion values', 'red'))
            missing_resources = missing_conversion.groupby(['product/resource']).count()
            print(missing_resources)

        # Join rates into the dataframe
        report_with_cost = pd.merge(left=report_with_conversion, right=rate_card, how='left',
                                    left_on='product/resource', right_on='Resource')

        #Check for non-zero usage without rates
        rate_check = pd.isna(report_with_cost['UnitPrice']) & report_with_cost['usage/billedQuantity'] != 0
        missing_rates = report_with_cost[rate_check]

        if missing_rates.shape[0] > 0:
            print(colored('The following resources are missing rates', 'red'))
            missing_resources = missing_rates.groupby(['product/resource']).count()
            print(missing_resources)

        report_with_cost['LineCost'] = report_with_cost['usage/billedQuantity'] * report_with_cost['Conversion'] * report_with_cost['UnitPrice']
        report_with_cost['FileId'] = filename
        report_with_cost.to_csv(destination_path_cost + '/' + filename.replace('.gz','').replace('.csv','_cost.csv'), index=False)
        # update progress marker
        with open(progress_made_file, 'w') as text_file:
            text_file.write(filename)

    else:
        print('Skipped file since it is already processed or file is too old.')