import logging
from azure.storage.blob import BlockBlobService
import pandas as pd
import datetime
from CrowdTangleAPI import Dashboard, api_url


def scrape_CrowdTangle_part1():
    logging.info('scrape_CrowdTangle_part1 has started')
    bbs = BlockBlobService(
        connection_string='DefaultEndpointsProtocol=https;AccountName=octagonsocialdata;AccountKey=XsuDBxapWwCMQgHib2GiS1Ii96f2+b6Gkjcu1+gPjrRg28zkPzNv1S6+JGkWIwGCHGbO2jkYo5NrVLb2tKEZqg==;EndpointSuffix=core.windows.net'
    )
    logging.info('bbs started')
    fb_api_token = 'vFIzJxjrUp83wKc2ZK6RhfGy3Eu3Ud0OCJaDkDcc'
    list_of_terms = [""]
    start_date_val = str(datetime.date.today() - datetime.timedelta(days=1))
    end_date_val = str(datetime.date.today())
    list_of_campaigns = [['futures_aus', 1422513],
                        ['futures_uk', 1423613],
                        ['futures_us', 1440032]]
    logging.info('dashboard about to be created')
    d = Dashboard(api_url)
    logging.info('dashboard created')
    
    # convert list to the string format needed for crowdtangle
    str_list = ''
    for i in list_of_terms:
        str_list += (i + ', ')
    terms = str_list[:-2]
    for campaign in list_of_campaigns:
        logging.info('Campaign: ')
        logging.info(campaign)


        facebook_posts = d.get_posts(start_date_val, campaign[1], fb_api_token, terms, end_date_val)
        logging.info(f"len(facebook_posts): {len(facebook_posts)}")
        fb_posts = []


        for i in facebook_posts:
            try:
                message = i['message'].lower()
            except:
                message = ''
            platform = 'Facebook'
            author = i['account']['name']
            profile_img = i['account']['profileImage']
            try:
                user_name = i['account']['handle']
            except:
                user_name = ''
            reach = i['account']['subscriberCount']
            try:
                post_id = i['platformId'].split('_')[1] # first part is user_id, but don't need
            except:
                post_id = i['platformId'].split(':')[0] # sometimes it is this format?
            time_stamp = i['date']
            text = message
            rts_shares = i['statistics']['actual']['shareCount']
            faves_likes = i['statistics']['actual']['likeCount']
            comments = i['statistics']['actual']['commentCount']
            media_type = i['type']
            try:
                link = i['link']
            except:
                link = ''
            try:
                media_url = i['media'][0]['url']
            except:
                media_url = ''
            quot_text = ''
            quot_img = ''
            fb_posts.append([platform,
                            author,
                            user_name,
                            reach,
                            post_id,
                            time_stamp,
                            text,
                            rts_shares,
                            faves_likes,
                            comments,
                            media_type,
                            link,
                            media_url,
                            quot_text,
                            quot_img,
                            profile_img
                            ])

        facebook_df = pd.DataFrame(columns= ['platform_name',
                                            'author',
                                            'user_name',
                                            'reach',
                                            'post_id',
                                            'time_stamp',
                                            'text',
                                            'rts_shares',
                                            'faves_likes',
                                            'comments',
                                            'media_type',
                                            'link',
                                            'media_url',
                                            'quot_text',
                                            'quot_img',
                                            'profile_img'],
                                data = fb_posts)
        logging.info(f'facebook_df shape: {facebook_df.shape}')
        bytes_to_write = facebook_df.to_csv(None).encode()
        blob_name = campaign[0] + "-" + str(datetime.date.today()) + "_accounts_Azure.csv"
        bbs.create_blob_from_bytes(
            container_name="philcontainertest",
            blob_name=blob_name,
            blob=bytes_to_write
        )
        logging.info(f"facebook_df written to: {blob_name}")


def main(name) -> str:
    logging.info(f'`name`: {name}')
    logging.info('scrape_CrowdTangle_part1 about to start')
    scrape_CrowdTangle_part1()

    return "Done"
