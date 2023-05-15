
import pandas as pd, numpy as np
from pandas.io.json import json_normalize
import uuid
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.api_core import protobuf_helpers
import proto
import sys
import re
import io, os
import time
import threading
from threading import Thread
import logging
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import traceback

class Google_ads_exact_volumes:

    def __init__(self, client, customer_id, country_code, language_code, last_date, duration_in_months, location_type="Country"):
        config_folder_path = "/Workspace/Repos/ishit@i-genie.ai/google_ads_exact_volume"
        self.geotarget_id = self.__get_location(config_folder_path,country_code)
        self.language_id = self.__get_language(config_folder_path,language_code)
        print(f"geotarget_id : {self.geotarget_id} , {type(self.geotarget_id)}")
        print(f"language_id : {self.language_id} , {type(self.language_id)}")
        self.client = client
        self.customer_id = customer_id
        self.last_date = last_date
        self.duration_in_months = duration_in_months
        self.location_type = location_type


    def __get_location(self,config_folder_path,country_code):
        try:
            locations_df = pd.read_csv(os.path.join(config_folder_path , "google_ads_locations.csv"))
            locations_df['Criteria ID']= locations_df['Criteria ID'].astype(str)
            country_criterion_id = locations_df.loc[(locations_df["Target Type"]=="Country") & (locations_df["Country Code"]==country_code), 'Criteria ID'].iloc[0]
            return country_criterion_id
        except Exception as e:
            raise e
        

    def __get_language(self,config_folder_path, language_code):
        try:
            languages_df = pd.read_csv(os.path.join(config_folder_path , "google_ads_languages.csv"))
            languages_df['Criterion ID'] = languages_df['Criterion ID'].astype(str)
            language_criterion_id = languages_df.loc[languages_df["Language code"]==language_code, 'Criterion ID'].iloc[0]
            return language_criterion_id
        except Exception as e:
            raise e


    def get_kw_metrics(self, keywords):
        try:
            #generate plan (including campaign, and ad group where keywords are stored)
            keyword_plan_info = self._add_keyword_plan()
            
            #get path names for plan, campaign, and ad group
            keyword_plan = keyword_plan_info[0]
            keyword_plan_campaign = keyword_plan_info[1]
            keyword_plan_ad_group = keyword_plan_info[2]

            #extract plan and campaign id from full path name (this part could def be cleaner)
            p = re.search('([^\/]+$)', keyword_plan)
            if p:
                keyword_plan_id = p.group(1)
        except Exception as e:
            raise e

        try:
            p = re.search('([^\/]+$)', keyword_plan_campaign)

            if p:
                keyword_plan_campaign_id = p.group(1)

            #adds list of keywords into ad group
            self._add_keywords(keyword_plan_ad_group, keywords)

            df_kw_metrics = pd.DataFrame()       

            #request historical metrics for current location
            df_kw_metrics_geo = self._request_kw_metrics(keyword_plan_id, keywords)

            #concat historical metrics to full dataframe
            df_kw_metrics = pd.concat([df_kw_metrics, df_kw_metrics_geo])
            df_kw_metrics = df_kw_metrics.reset_index()
            df_kw_metrics = df_kw_metrics.drop('index', axis = 1)
            #time.sleep(1)

            #delete keyword plan
            self._delete_keyword_plan(keyword_plan_id)

            return df_kw_metrics
        except Exception as e:
            print(f"Error occured : {traceback.format_exc()}")
            self._delete_keyword_plan(keyword_plan_id)
            raise e
    
    def _add_keyword_plan(self):
        """
        Adds a keyword plan, campaign, ad group, etc. to the customer account.
        Raises:
            GoogleAdsException: If an error is returned from the API.
        """
        try:
            #generate plan and return plan path name
            keyword_plan = self._create_keyword_plan()
            
            #generates the ad campaign
            keyword_plan_campaign = self._create_keyword_plan_campaign(keyword_plan)
            
            #generates the ad group
            keyword_plan_ad_group = self._create_keyword_plan_ad_group(keyword_plan_campaign)
            
            #store new keyword plan path names in list to return
            keyword_plan_info = [keyword_plan, keyword_plan_campaign, keyword_plan_ad_group]
            
            return keyword_plan_info
        except Exception as e:
            raise e
        
    
    def _create_keyword_plan(self):
        """
        Adds a keyword plan to the given customer account.

        Returns:
            A str of the resource_name for the newly created keyword plan.

        Raises:
            GoogleAdsException: If an error is returned from the API.
                
        """ 
        try:  
            keyword_plan_service = self.client.get_service("KeywordPlanService")
            operation = self.client.get_type("KeywordPlanOperation")
            keyword_plan = operation.create
            
            keyword_plan.name = f"Keyword plan for traffic estimate {uuid.uuid4()}"
            
            forecast_interval = (
                self.client.enums.KeywordPlanForecastIntervalEnum.NEXT_QUARTER
            )
            keyword_plan.forecast_period.date_interval = forecast_interval
            
            response = keyword_plan_service.mutate_keyword_plans(
                customer_id=self.customer_id, operations=[operation]
            )
            
            resource_name = response.results[0].resource_name
            
            print(f"Created keyword plan with resource name: {resource_name}")

            return resource_name
        except Exception as e:
            raise e


    def _create_keyword_plan_campaign(self, keyword_plan):
        """
        Adds a keyword plan campaign to the given keyword plan.

        Args:
            keyword_plan: A str of the keyword plan resource_name this keyword plan
                campaign should be attributed to.create_keyword_plan.

        Returns:
            A str of the resource_name for the newly created keyword plan campaign.

        Raises:
            GoogleAdsException: If an error is returned from the API.
        """

        try:
            keyword_plan_campaign_service = self.client.get_service("KeywordPlanCampaignService")
            operation = self.client.get_type("KeywordPlanCampaignOperation")
            keyword_plan_campaign = operation.create

            keyword_plan_campaign.name = f"Keyword plan campaign {uuid.uuid4()}"
            keyword_plan_campaign.cpc_bid_micros = 10000
            keyword_plan_campaign.keyword_plan = keyword_plan

            network = self.client.enums.KeywordPlanNetworkEnum.GOOGLE_SEARCH
            keyword_plan_campaign.keyword_plan_network = network

            geo_target = self.client.get_type("KeywordPlanGeoTarget")
            # Constant for U.S. Other geo target constants can be referenced here:
            # https://developers.google.com/google-ads/api/reference/data/geotargets
            #geo_target.geo_target_constant = "geoTargetConstants/2840"
            geo_target.geo_target_constant = f"geoTargetConstants/{self.geotarget_id}"
            keyword_plan_campaign.geo_targets.append(geo_target)

            # Constant for English
            #language = "languageConstants/1000"
            language = f"languageConstants/{self.language_id}"
            keyword_plan_campaign.language_constants.append(language)
            response = keyword_plan_campaign_service.mutate_keyword_plan_campaigns(customer_id=self.customer_id, operations=[operation])
            resource_name = response.results[0].resource_name

            print(f"Created keyword plan campaign with resource name: {resource_name}")

            return resource_name
        except Exception as e:
            raise e

    
    def _create_keyword_plan_ad_group(self, keyword_plan_campaign):
        """
        Adds a keyword plan ad group to the given keyword plan campaign.

        Args:
            client: An initialized instance of GoogleAdsClient
            customer_id: A str of the customer_id to use in requests.
            keyword_plan_campaign: A str of the keyword plan campaign resource_name
                this keyword plan ad group should be attributed to.

        Returns:
            A str of the resource_name for the newly created keyword plan ad group.

        Raises:
            GoogleAdsException: If an error is returned from the API.
        """
    
        try:
            #print("_create_keyword_plan_ad_group")
            operation = self.client.get_type("KeywordPlanAdGroupOperation")
            keyword_plan_ad_group = operation.create

            keyword_plan_ad_group.name = f"Keyword plan ad group {uuid.uuid4()}"
            keyword_plan_ad_group.cpc_bid_micros = 10000
            keyword_plan_ad_group.keyword_plan_campaign = keyword_plan_campaign

            keyword_plan_ad_group_service = self.client.get_service(
                "KeywordPlanAdGroupService"
            )
            response = keyword_plan_ad_group_service.mutate_keyword_plan_ad_groups(
                customer_id=self.customer_id, operations=[operation]
            )

            resource_name = response.results[0].resource_name

            print(f"Created keyword plan ad group with resource name: {resource_name}")

            return resource_name
        except Exception as e:
            raise e

    
    def _add_keywords(self, plan_ad_group, keywords):

        try:
            keyword_plan_ad_group_keyword_service = self.client.get_service( "KeywordPlanAdGroupKeywordService")
            operation = self.client.get_type("KeywordPlanAdGroupKeywordOperation")
            operations = []
            
            #for each keyword, add to create operation 
            for keyword in keywords:
                operation = self.client.get_type("KeywordPlanAdGroupKeywordOperation")
                keyword_plan_ad_group_keyword = operation.create
                keyword_plan_ad_group_keyword.text = keyword
                keyword_plan_ad_group_keyword.cpc_bid_micros = 10000
                keyword_plan_ad_group_keyword.match_type = (self.client.enums.KeywordMatchTypeEnum.BROAD)
                keyword_plan_ad_group_keyword.keyword_plan_ad_group = plan_ad_group
                operations.append(operation)

            flag = 1
            while flag<=len(operations):
                try:
                    print(f"adding keywords {flag} times. operations : {len(operations)}")
                    response = keyword_plan_ad_group_keyword_service.mutate_keyword_plan_ad_group_keywords(
                    customer_id=self.customer_id, operations=operations)
                    time.sleep(3)
                    flag=len(operations)+1
                except GoogleAdsException as ex:
                    flag+=1
                    not_copy = []
                    for error in ex.failure.errors:
                        if error.message == "The required repeated field was empty.":
                            flag=len(operations)+1
                            #raise ex
                        elif error.location:  
                            print("="*100)
                            print(f"\t {error.message}")
                            print(f"\t\tOn field: {field_path_element.field_name} -> {error.trigger.string_value}")
                            print(f"\t\tOn index: {field_path_element.index}")
                            not_copy = []
                            for field_path_element in error.location.field_path_elements:
                                if field_path_element.field_name == "operations":
                                    not_copy.append(field_path_element.index)

                    not_copy_sorted = sorted(not_copy, reverse=True)
                    print("Not copy : ",not_copy_sorted)
                    for index in not_copy_sorted:
                        if index < len(operations):
                            operations.pop(index)
            
            resource_names = [result.resource_name for result in response.results]
        except Exception as e:
            raise e


    def _request_kw_metrics(self,keyword_plan_id, keywords):
        try:
            keyword_plan_service = self.client.get_service("KeywordPlanService")

            #get path name from keyword plan
            resource_name = keyword_plan_service.keyword_plan_path(self.customer_id, keyword_plan_id)

            #define time range
            end_date = self.last_date - relativedelta(months=1)
            start_date = end_date - relativedelta(months=self.duration_in_months)

            start_year = start_date.year
            start_month = start_date.strftime("%B").upper()

            end_year = end_date.year
            end_month = end_date.strftime("%B").upper()
            
            print(f"start_date : {start_year} - {start_month}")
            print(f"end_date : {end_year} - {end_month}")

            date_range = {'start':{'year':start_year,
                                    'month':start_month},
                        'end':{'year':end_year,
                                'month':end_month}}

            year_month_range = {'year_month_range':date_range}

            request = {'keyword_plan':resource_name,'historical_metrics_options':year_month_range}
            #generate historical metrics for keywords
            #response comes as a google protobuf object (protocol buffering?)
            response = keyword_plan_service.generate_historical_metrics(request = request)

            #convert protobuf to dict
            response_dict = proto.Message.to_dict(response)    
            response_dict = response_dict['metrics']

            #converts the dict to a dataframe, but cols still need to be parsed
            df_response = pd.DataFrame.from_dict(response_dict, orient = 'columns')

            has_metrics = 'keyword_metrics' in df_response

            if has_metrics:

                #explode the json column of keyword metrics and add that back to the original output df
                #fill nan values in metrics col with empty dict (to allow json normalize)
                df_response['keyword_metrics'] = df_response['keyword_metrics'].fillna({i: {} for i in df_response.index})

                kw_metrics = pd.json_normalize(df_response['keyword_metrics'])

                df_response = df_response.drop('keyword_metrics', axis = 1)

                df_kw_metrics = pd.concat([df_response, kw_metrics], axis = 1)

                #filter out kw with nan data
                df_kw_metrics_nan = df_kw_metrics[df_kw_metrics['monthly_search_volumes'].isna()]
                df_kw_metrics_nan = df_kw_metrics_nan[['search_query']]

                df_kw_metrics = df_kw_metrics[~df_kw_metrics['monthly_search_volumes'].isna()].reset_index(drop = True)

                df_kw_search_vol = pd.DataFrame()

                #for each row, this explodes the monthly search volumes into individual columns
                for i in range(len(df_kw_metrics)):

                    kw_search_vols_list = df_kw_metrics['monthly_search_volumes'][i]

                    d = {}
                    for k in kw_search_vols_list[0].keys():
                        d[k] = tuple(d[k] for d in kw_search_vols_list)

                    kw_search_vol = pd.DataFrame.from_dict(d, orient = 'index')
                    kw_search_vol.loc['month'] = kw_search_vol.loc['month'].astype(int)

                    #subtract 1 from each month (output has January as month 2 and so on)
                    kw_search_vol.loc['month'] = kw_search_vol.loc['month'] - 1

                    kw_search_vol.loc['month'] = kw_search_vol.loc['month'].astype(str)

                    #add 0 to single digit months
                    single_digit_month = kw_search_vol.loc['month'].str.len() == 1
                    kw_search_vol.loc['month'][single_digit_month] = '0' + kw_search_vol.loc['month'][single_digit_month]

                    kw_search_vol.loc['year'] = kw_search_vol.loc['year'].astype(str)
                    date_cols = kw_search_vol.loc['year'] + '-' + kw_search_vol.loc['month']
                    kw_search_vol.columns = date_cols
                    kw_search_vol = kw_search_vol.drop(['month', 'year'], axis = 0)
                    df_kw_search_vol = pd.concat([df_kw_search_vol, kw_search_vol], axis = 0)    

                df_kw_search_vol = df_kw_search_vol.reset_index(drop = True)

                df_kw_metrics = df_kw_metrics[['search_query', 'avg_monthly_searches']]

                df_kw_metrics = pd.concat([df_kw_metrics, df_kw_search_vol], axis = 1)

                #add back in keywords with nan results
                df_kw_metrics['nan_results'] = False
                df_kw_metrics_nan['nan_results'] = True
                df_kw_metrics = pd.concat([df_kw_metrics, df_kw_metrics_nan], axis = 0)

                #adds geotargeting info to df
                df_kw_metrics['location_type'] = self.location_type
                df_kw_metrics['location_id'] = self.geotarget_id

                #add in kw batch data        
                #df_kw_metrics = df_kw_metrics.merge(kw_batch, on = 'search_query')


            elif has_metrics == False:

                #if no metrics, generate dataframe with null volumes
                df_kw_metrics = pd.DataFrame(data = {'search_query':keywords,
                                                    'avg_monthly_searches':np.nan,
                                                    'nan_results':True,
                                                    'location_type':self.location_type,
                                                    'location_id':self.geotarget_id
                                                    })

            print(f"Got metrics for keyword plan with resource name: {resource_name} at " + self.location_type + ', ' + self.geotarget_id)
            return df_kw_metrics

        except Exception as e:
            raise e
        
    def _delete_keyword_plan(self,keyword_plan_id):
        try:
            keyword_plan_service = self.client.get_service("KeywordPlanService")
            keyword_plan_operation = self.client.get_type("KeywordPlanOperation")

            resource_name = keyword_plan_service.keyword_plan_path(self.customer_id, keyword_plan_id)
            keyword_plan_operation.remove = resource_name

            response = keyword_plan_service.mutate_keyword_plans(customer_id=self.customer_id, operations=[keyword_plan_operation])
            
            resource_name = response.results[0].resource_name
            
            print(f"Deleted keyword plan with resource name: {resource_name}")
        except Exception as e:
            raise e