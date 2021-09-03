import logging
import datetime
from azure.storage.blob import BlockBlobService
import pandas as pd
import time
import requests

def scrape_CrowdTangle_part3():
    logging.info('scrape_CrowdTangle_part3 has started')
    bbs = BlockBlobService(
        connection_string='DefaultEndpointsProtocol=https;AccountName=octagonsocialdata;AccountKey=XsuDBxapWwCMQgHib2GiS1Ii96f2+b6Gkjcu1+gPjrRg28zkPzNv1S6+JGkWIwGCHGbO2jkYo5NrVLb2tKEZqg==;EndpointSuffix=core.windows.net'
    )
    insta_api_token = 'T8rE357isKx2WzigmPznBXqvC7rsZAfS4AyGvYsy'
    ###################################
    #                                 #
    #      SEARCH EXPORT              #
    #                                 #
    ###################################

    man_u_search_terms = 'manchester united,man utd,red devil,old trafford,bruno fernandes,marcus rashford,ole solskj,alex ferguson,ed woodward,david de gea,victor lindel,harry maguire,paul pogba,edinson cavani,juan mata,anthony martial,mason greenwood,luke shaw,dean henderson,alex telles,wan bissaka,nemanja matic,van de beek,scott mcTominay'
    #formula_e_search_terms = 'abbFormulae, formula e, formule e, formel e, #abbformulae, #fiaformulae, @ABBFormulaE'
    supercars_search_terms = 'V8 Supercars, Repco Supercars, #RepcoSC, #V8SC, #Supercars, @Supercars, @supercarschampionship, #supercarschampionship, Red Bull Ampol, Erebus Motorsport, Kelly Racing, Kelly Grove, Irwin Racing, DeWalt Racing, Brad Jones Racing, Matt Stone Racing, Walkinshaw Andretti, Tickford Racing, DJR Racing, Triple Eight, Team 18, Team Sydney, Shane Van Ginsbergen, Nick Percat, Mark Winterbottom, Chaz Mostert, Jamie Whincup, Anton de Pasquale, Cam Waters, Jack Le Brocq'
    formula_e_search_terms = 'abbFormulae, #abbformulae, #fiaformulae, @ABBFormulaE, @fiaformulae, Formula E, Formel E, Formule E, #Diriyaheprix, @envisionvirginracing, @tagheuerporschefe, @dstecheetah, @mahindraracing'

    nrl_search_terms = "nrl, rugby league, state of origin, telstra premiership, magic round, peter v'lys, commission v'lys, rew abdo, abdo ceo, abdo executive, brisbane broncos, canterbury bulldogs, canberra raiders, cronulla sharks, melbourne storm, manly sea eagles, parramatta eels, south rabbitohs, wests tigers, queensl cowboys, penrith panthers, sydney roosters, zeal warriors, warriors tamworth, gold coast titans, newcastle knights, anz stadium rabbitohs, warriors mt smart stadium, anz stadium bulldogs, sydney cricket ground roosters, scg roosters, mcdonald jones stadium knights, broncos suncorp stadium, aami park storm, queensl country bank stadium cowboys, lottol sea eagles, cbus super stadium titans, gio stadium raiders, leichhardt oval tigers, netstrata jubilee stadium dragons, netstrata jubilee stadium sharks) OR (win stadium dragons, panthers stadium, broncos haas, broncos milford, broncos pangaii, broncos seibold, anthony seibold, broncos boyd, broncos glenn, titans arrow, titans taylor, ash taylor, titans proctor, titans holbrook, justin Holbrook, titans james, ryan james, mal meninga, titans meninga, roosters robinson, trent Robinson, cooper cronk, roosters cronk, james tedesco, roosters tedesco, boyd cordner, roosters cordner, roosters premiers, roosters keary, roosters morris, roosters crichton, roosters friend, roosters radley, victor radley, brad Arthur, eels arthur, clint gutherson, eels gutherson, blake Ferguson, eels ferguson, maika sivo, eels sivo, mitchell moses, eels moses, rugby rlc, rugby arl, bankwest stadium"

    emls_search_terms = '#eMLS, #eMLSCup, #eMLSSeries1, #eMLSSeries2, eMLS CUP, eMLS, @eMLS, @xbleu7, @didychrislito'

    lancashire_search_terms = 'emirates old Trafford , emiratesot , lancashire cricket , lancashire lightning , lancashire ccc , lancashire thunder , lancashire county cricket club , lancscricket, #RedRoseTogether, lccc , lancashirecricket , lancs CCC , lancsccc , lancashire'

    football_australia_terms = 'adelaide united  ,a-league  adelaide  united , a-league  adelaide  reds ,a-league  adelaide  united ,a-league  adelaide  reds , a-league  adelaide  united , a-league  adelaide  reds , ladder  adelaide  united , soccer  adelaide  united , w-league  adelaide  united , w-league  adelaide  reds , w-league  adelaide  united , w-league  adelaide  reds , w-league  adelaide  united , w-league  adelaide  reds , brisbane roar  ,a-league  brisbane  roar , a-league  roar , a-league  brisbane  roar , a-league  roar , ladder  brisbane  roar , soccer  brisbane  roar ,a-league  brisbane  roar ,a-league  roar , w-league  brisbane  roar , w-league  roar , w-league  brisbane  roar , w-league  roar , w-league  brisbane  roar , w-league   roar, central coast mariners  ,a-league  central  mariners , a-league  mariners , a-league  central  mariners , a-league  mariners , ladder  central  mariners , soccer  central  mariners ,a-league  central  mariners ,a-league  mariners , macarthur fc  ,a-league  macarthur  fc , a-league  macarthur , ladder  macarthur  fc , soccer  macarthur  fc , a-league  macarthur  fc , a-league  macarthur ,a-league  macarthur  fc ,a-league  macarthur ,  a-league  melbourne  city , a-league  city , ladder  melbourne  city , soccer  melbourne  city , a-league  melbourne  city , a-league  city ,a-league  melbourne  city ,a-league  city , w-league  melbourne  city , w-league  city , w-league  melbourne  city , w-league  city , w-league  melbourne  city , w-league  city , melbourne victory  ,a-league  melbourne  victory , a-league  victory , ladder  melbourne  victory , soccer  melbourne  victory , a-league  melbourne  victory , a-league  victory ,a-league  melbourne  victory ,a-league  victory , w-league  melbourne  victory , w-league  victory , w-league  melbourne  victory , w-league  victory , w-league  melbourne  victory , w-league  victory , newcastle jets  ,a-league  newcastle  jets , a-league  jets , ladder  newcastle  jets , soccer  newcastle  jets , a-league  newcastle  jets , a-league  jets ,a-league  newcastle  jets ,a-league  jets , w-league  newcastle  jets , w-league  jets , w-league  newcastle  jets , w-league  jets , w-league  newcastle  jets , w-league  jets , perth glory  ,a-league  perth  glory , a-league  glory , ladder  perth  glory , soccer  perth  glory , a-league  perth  glory , a-league  glory ,a-league  perth  glory ,a-league  glory , w-league  perth  glory , w-league  glory , w-league  perth  glory , w-league  glory , w-league  perth  glory , w-league  glory , sydney fc  ,a-league  sydney  fc , a-league  sky blues , ladder  sydney  fc , soccer  sydney  fc , a-league  sydney  fc , a-league  sky blues ,a-league  sydney  fc ,a-league  sky blues , ladder  sydney  fc , soccer  sydney  fc , w-league  sydney  fc , w-league  sydney  fc , w-league  sydney  fc , wellington phoenix  ,a-league  wellington  phoenix , a-league  phoenix , ladder  wellington  phoenix , soccer  wellington  phoenix , a-league  wellington  phoenix , a-league  phoenix ,a-league  wellington  phoenix ,a-league  phoenix , western sydney werers  ,a-league  western  werers , a-league  werers , ladder  western  werers , soccer  western  werers ,a-league  western  werers ,a-league  werers , a-league  western  werers , a-league  werers , w-league  western  werers , w-league  werers , w-league  western  werers , w-league  werers , w-league  western  werers , w-league  werers , western united  , a-league   western  united , ladder  western  united , soccer   western  united , a-league   western  united ,a-league   western  united'

    atl_falcons_terms = 'AtlantaFalcons , ATLUTD , MBStadium , ATLUTD2 , VAMOSATLUTD , thdbackyard , TheFactionATL , AcademyATLUTD , ResurganceATL , DirtySouthSoc , FootieMob , _moadams , AntonWalkes , _milesrobinson_ , JosefMartinez17 , atlutdpup , celebrationbowl , DEalesATLUTD , Francoeescobar , M_Ryan02 , MercedesBenzUSA , BocaBoca3 , CFAPEachBowl , ATLFalconsUK , CalvinRidley1 , TailgateTeam , #RiseUpATL , #Falcons , #ATLUTD , #AtlantaUnited , #AtlantaFalcons , #MattyIce , #UniteAndConquer , #FiveStripeFriday , #5StripeFriday , #MercedesBenzStadium , #MBStadium , #VamosATL , #ATLUnitedFC , #ATLUnited , #ATLSoccer , #ATLUTD , Falcons , Atlanta Falcons ,  Mercedes-Benz Stadium , Mercedes Benz Stadium ,  Atlanta United  , ATL United , @AtlantaFalcons , @ATLUTD , @MBStadium , @ATLUTD2 , @VamosATLUTD'

    all_str_terms = '@MLB , @AllStarGame , @TMobile , @Mastercard , @Gatorade , @chevrolet , @FreddieFreeman5 , @tatis_jr , @ronaldacunajr24 , @mookiebetts , @BusterPosey , @ozzie , @KrisBryant_23 , @JTRealmuto , @treavturner , @bcraw35 , @JuanSoto25_ , @B_Woody24 , @Max_Scherzer , @faridyu , @BauerOutage , @Kimbrel46 , @Mark_Melancon_ , @SalvadorPerez15 , @MikeTrout , @TheJudge44 , @GerritCole45 , @TeamCJCorrea , @JDMartinez28 , @ShaneBieber19 , @AChapman_105 , @Dbacks , @Braves , @Orioles , @RedSox , @whitesox , @Cubs , @Reds , @Indians , @Rockies , @tigers , @astros , @Royals , @Angels , @Dodgers , @Marlins , @Brewers , @Twins , @Yankees , @Mets , @Athletics , @Phillies , @Pirates , @Padres , @SFGiants , @Mariners , @Cardinals , @RaysBaseball , @Rangers , @BlueJays , @Nationals , #HRDerby , #AllStarBallot , #MLBVote , #StandUpToCancer , #SU2C , #AllStarGame , MLB , All Star Weekend , MLB All-Star Weekend , MLB All Star Game , MLB All-Star Game , T Mobile Home Run Derby , T-Mobile Home Run Derby , Gatorade All Star Workout Day , Gatorade All-Star Workout Day , All Star Game presented by Mastercard , All-Star Game presented by Mastercard , Chevrolet MLB All Star Game MVP , Chevrolet MLB All-Star Game MVP , Midsummer Classic , 2021 Midsummer Classic'

    packers_terms = 'Packers, Green Bay Pack, #GoPackGo , @packers , #packers'

    evo_terms = '#EVO2021 , EVO2021 , @EVO , EVO2021 , EVO Championship Series'

    search_list_of_campaigns = [
                                    ['ManUtd_2020-21-season', man_u_search_terms, 3],
                                    ['FormulaE_2021-season', formula_e_search_terms, 5],
                                    ['Supercars_2021-season', supercars_search_terms, 5, 'AU,NZ'],
                                    ['NRL_2021-season', nrl_search_terms, 5, 'AU,NZ'],
                                    ['PlayStation_2021-eMLS-Cup', emls_search_terms, 5],
                                    ['Lancashire_2021', lancashire_search_terms, 4],
                                    ['FootballAus_2020-21-Season', football_australia_terms, 5, 'AU,NZ'],
                                    ['ATLFalcons_2020-21-season', atl_falcons_terms, 3],
                                    ['Packers_2021-22-season', packers_terms, 5],
                                    ['EVO_2021-22-season', evo_terms, 5],
                                    ['MLB_AllStar_2021', all_str_terms, 5]
                                ]
    ## 3rd value in lists above is the number of days going back


    all_posts_list = []


    headers = {
    'Accept': 'application/json',
    'x-api-token': insta_api_token,
    }

    time.sleep(12)
    logging.info("about to loop through search list of campaigns")
    for camp in search_list_of_campaigns:
        logging.info(f'Campaign: {camp[0]}')
        offset = 0

        while offset <= 900:
            try:
            #logging.info(offset)
                if len(camp) == 4:
                    params = (
                        ('count', 100),
                        ('offset', offset),
                        ('startDate', (datetime.date.today() - datetime.timedelta(camp[2])).isoformat()),
                        ('searchTerm', camp[1]),
                        ('sortBy', 'total_interactions'),
                        ('platforms', "facebook,instagram"),
                        ('pageAdminTopCountry', camp[3])
                    )
                else:
                    params = (
                        ('count', 100),
                        ('offset', offset),
                        ('startDate', (datetime.date.today() - datetime.timedelta(camp[2])).isoformat()),
                        ('searchTerm', camp[1]),
                        ('sortBy', 'total_interactions'),
                        ('platforms', "facebook,instagram")
                    )

                response = requests.get('https://api.crowdtangle.com/posts/search',
                                            headers=headers, params=params)
                resp = response.json()
                #logging.info(len(resp['result']['posts']))

                for i in resp['result']['posts']:

                    indiv_post = []
                    campaign_name = camp[0]
                    platformId = i['platformId']
                    platform = i['platform']
                    date = i['date']
                    updated = i['updated']
                    post_type = i['type']
                    try:
                        post_description = i['description']
                    except:
                        post_description = ''
                    try:
                        message = i['message']
                    except:
                        message = ''
                    try:
                        link = i['link']
                    except:
                        link = ''
                    postUrl = i['postUrl']
                    subscriberCount = i['subscriberCount']
                    score = i['score']
                    try:
                        media_type = i['media'][0]['type']
                    except:
                        media_type = ''
                    try:
                        media_url = i['media'][0]['url']
                    except:
                        media_url = ''
                    try:
                        likeCount = i['statistics']['actual']['likeCount']
                    except:
                        like_count = likeCount = i['statistics']['actual']['favoriteCount']
                    try:
                        shareCount = i['statistics']['actual']['shareCount']
                    except:
                        shareCount = 0
                    try:
                        commentCount = i['statistics']['actual']['commentCount']
                    except:
                        commentCount = 0
                    account_id = i['account']['id']
                    account_name = i['account']['name']
                    account_subscriberCount = i['account']['subscriberCount']
                    account_url = i['account']['url']
                    try:
                        account_accountType = i['account']['accountType']
                    except:
                        account_accountType = ''

                    indiv_post.extend([
                                    campaign_name,
                                    platformId,
                                    platform,
                                    date,
                                    updated,
                                    post_type,
                                    post_description,
                                    message,
                                    link,
                                    postUrl,
                                    subscriberCount,
                                    score,
                                    media_type,
                                    media_url,
                                    likeCount,
                                    shareCount,
                                    commentCount,
                                    account_id,
                                    account_name,
                                    account_subscriberCount,
                                    account_url,
                                    account_accountType])
                    all_posts_list.append(indiv_post)
            # logging.info(len(all_posts_list))
            # logging.info('--')
                offset += 100
                time.sleep(12)
            except:
                offset += 100
                time.sleep(12)
                logging.info('SEARCH EXCEPTION')
                logging.info(offset)
                try:
                    logging.info(response)
                except:
                    logging.info('couldnt logging.info response')

    posts_df = pd.DataFrame(data = all_posts_list,
                        columns = [
                        'campaign_name',
                        'platformId',
                        'platform',
                        'date',
                        'updated',
                        'post_type',
                        'post_description',
                        'message',
                        'link',
                        'postUrl',
                        'subscriberCount',
                        'score',
                        'media_type',
                        'media_url',
                        'likeCount',
                        'shareCount',
                        'commentCount',
                        'account_id',
                        'account_name',
                        'account_subscriberCount',
                        'account_url',
                        'account_accountType'])
    logging.info(f'posts_df shape: {posts_df.shape}')
    blob_name = "search_ig_and_fb" + "-" + str(datetime.date.today()) + "_search_Azure.csv"
    bytes_to_write = posts_df.to_csv(None).encode()
    bbs.create_blob_from_bytes(
        container_name="philcontainertest",
        blob_name=blob_name,
        blob=bytes_to_write
    )
    logging.info(f"posts_df written to: {blob_name}")


def main(name) -> str:
    logging.info(f'`name`: {name}')
    logging.info('scrape_CrowdTangle_part3 about to start')
    scrape_CrowdTangle_part3()

    return "Done"
