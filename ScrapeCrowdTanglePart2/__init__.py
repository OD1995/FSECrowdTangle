import logging
import datetime
from azure.storage.blob import BlockBlobService
from CrowdTangleAPI import Dashboard, api_url
import pandas as pd

def scrape_CrowdTangle_part2():
    logging.info('scrape_CrowdTangle_part2 has started')
    bbs = BlockBlobService(
        connection_string='DefaultEndpointsProtocol=https;AccountName=octagonsocialdata;AccountKey=XsuDBxapWwCMQgHib2GiS1Ii96f2+b6Gkjcu1+gPjrRg28zkPzNv1S6+JGkWIwGCHGbO2jkYo5NrVLb2tKEZqg==;EndpointSuffix=core.windows.net'
    )
    d = Dashboard(api_url)
    insta_api_token = 'T8rE357isKx2WzigmPznBXqvC7rsZAfS4AyGvYsy'
    list_of_terms = [""]
    str_list = ''
    for i in list_of_terms:
        str_list += (i + ', ')
    terms = str_list[:-2]
    start_date_val = str(datetime.date.today() - datetime.timedelta(days=1))
    end_date_val = str(datetime.date.today())
    list_of_ig_campaigns = [['futures_ig_uk', 1462837 ],
                            ['futures_ig_aus', 1462861]]

    for campaign in list_of_ig_campaigns:
        logging.info('Instagram campaign: ')
        logging.info(campaign)

        insta_posts = d.get_posts(start_date_val, campaign[1], insta_api_token, terms, end_date_val)
        logging.info(f"len(insta_posts): {len(insta_posts)}")

        instagram_posts = []

        for i in insta_posts:
            try:
                description = i['description'].lower()
            except:
                description = ''
            platform = 'Instagram'
            author = i['account']['name']
            profile_img = i['account']['profileImage']
            user_name = i['account']['handle']
            reach = i['account']['subscriberCount']
            post_id = i['postUrl'].replace('https://www.instagram.com/p/', '').replace('/', '') # first part is user_id, but don't need
            time_stamp = i['date']
            text = description
            rts_shares = 0
            link = i['postUrl']
            faves_likes = i['statistics']['actual']['favoriteCount']
            comments = i['statistics']['actual']['commentCount']
            media_type = i['type']
            if media_type == 'photo' or media_type == 'album':
                try:
                    media_url = i['media'][0]['url']
                except:
                    media_url = ''
            elif media_type == 'video':
                try:
                    media_url = i['media'][1]['url'] # for vid it gives vid url first, then photo
                except: media_url = ''

            quot_text = ''
            quot_img = ''
            instagram_posts.append([platform,
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
                                data = instagram_posts)
        logging.info(f'facebook_df shape (actually insta): {facebook_df.shape}')
        bytes_to_write = facebook_df.to_csv(None).encode()
        blob_name = campaign[0] + "-" + str(datetime.date.today()) + "_accounts_Azure.csv"
        bbs.create_blob_from_bytes(
            container_name="philcontainertest",
            blob_name=blob_name,
            blob=bytes_to_write
        )
        logging.info(f"facebook_df (actually insta) written to: {blob_name}")

def main(name) -> str:
    logging.info(f'`name`: {name}')
    logging.info('scrape_CrowdTangle_part2 about to start')
    scrape_CrowdTangle_part2()

    return "Done"
