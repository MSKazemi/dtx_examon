import sys
sys.path.append("/home/seyedkazemi/codes/mskhelper/")
sys.path.append("/home/seyedkazemi/codes/examon-dtx/m100dxt/examon-PIII/")
import logging
from examon.examon import Examon, ExamonQL
import pytz
import pandas as pd
import mohsenutils2
import time
import multiprocessing
import imp
import datetime
import os
from configparser import ConfigParser
imp.reload(mohsenutils2)

def check_dir(path):
    if not os.path.isdir(path):
        os.makedirs(path)

def metrics_chunks(metrics_path, number_rows_chunks):
    metrics = pd.read_csv(metrics_path)
    mrt_chunks = [metrics[i:i+number_rows_chunks].copy()
                  for i in range(0, metrics.shape[0], number_rows_chunks)]
    return mrt_chunks

def dxt(sq, tstart, tstop, metric):
    df = sq.SELECT('*').FROM(metric).WHERE(cluster='marconi100').TSTART(tstart).TSTOP(tstop).execute().df_table     
    return df


def csv_save_data(data, path, file_name):
    if not os.path.isdir(path):
        logger.info('There is no path ==>'+str(path))
        os.makedirs(path)
        logger.info('Path created! ==> '+str(path))
    try:
        file_name_checked = mohsenutils2.csv_file_name_creator(path, file_name)
        if file_name_checked != file_name:
            logger.info('There is file with same name - '+str(file_name) + ' - so the file name updated to'+str(file_name_checked))
        data.to_csv(path+'/'+file_name_checked, compression='gzip', encoding='utf-8')
    except Exception as e:
        logger.error('Error'+str(e))


def check_follow_file(pth, follow_file):
    if not os.path.isfile(pth + follow_file):
        if not os.path.isdir(pth):
            logger.info('There is no path ==>'+str(pth))
            os.makedirs(pth)
            logger.info('Path created! ==> '+str(pth))
        pd.DataFrame([['2020-03-09 00:00:00', 0, 0]], columns=['datetime', 'row', 'col']).to_csv(pth + follow_file, index=False)
        logger.info(pth + follow_file+' created')


def backup_datetime(pth_follow_file):
    dt = pd.read_csv(pth_follow_file)
    dt.sort_values('datetime', inplace=True)
    dt = dt.tail(1)['datetime'].values[0]
    start = datetime.datetime.strftime(datetime.datetime.strptime(dt, '%Y-%m-%d %H:%M:%S'), '%d-%m-%Y %H:%M:%S')
    stop = datetime.datetime.strftime(datetime.datetime.now(pytz.timezone('Europe/Rome')), '%d-%m-%Y')+' 00:00:00'
    logger.info('backup_datetime start: '+str(start)+' stop: '+str(stop))
    return start, stop



def body_dxt(metrics):
    configur = ConfigParser()
    configur.read('../config.ini')
    KAIROSDB_SERVER = configur.get('examon_config','KAIROSDB_SERVER')
    KAIROSDB_PORT = configur.get('examon_config','KAIROSDB_PORT')
    USER = configur.get('examon_config','USER')
    PWD = configur.get('examon_config','PWD')
    
    ex = Examon(KAIROSDB_SERVER, port=KAIROSDB_PORT, user=USER, password=PWD, verbose=False, proxy=True)
    sq = ExamonQL(ex)
    
    global logger
    logger = logging.getLogger(str(os.getpid()))
    logger.setLevel(logging.INFO)
    log_file_name = str(os.getpid()) + '_' + datetime.datetime.strftime(datetime.datetime.now(pytz.timezone('Europe/Rome')), '%Y-%m-%d').replace(':','-').replace(' ','_')+'.log'
    formatter = logging.Formatter('%(levelname)s:%(process)d:%(processName)s:%(thread)d:%(threadName)s:%(asctime)s:%(lineno)d:%(message)s')

    if not logger.handlers:
        stream_handler = logging.StreamHandler()
        file_handler = logging.FileHandler(loggin_path + log_file_name)
        stream_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler) 








    # logger = logging.getLogger(str(os.getpid()))
    # logger.setLevel(logging.INFO)
    
    # log_file_name = str(os.getpid()) + '_' + datetime.datetime.strftime(datetime.datetime.now(pytz.timezone('Europe/Rome')), '%Y-%m-%d').replace(':','-').replace(' ','_')+'.log'

    # stream_handler = logging.StreamHandler()
    # file_handler = logging.FileHandler(loggin_path + log_file_name)
    
    # formatter = logging.Formatter('%(levelname)s:%(process)d:%(processName)s:%(thread)d:%(threadName)s:%(asctime)s:%(lineno)d:%(message)s')

    # stream_handler.setFormatter(formatter)
    # file_handler.setFormatter(formatter)
    # logger.addHandler(file_handler)
    # logger.addHandler(stream_handler) 



    for _, row in metrics.iterrows():
        plugin, metric = row[1], row[0]
        logger.info(str(plugin)+' -- '+str(metric)) 
        # follow_file = pth_data_files+'/follow/'+plugin+'_'+metric+'_follow_dxt.csv'
        follow_file = plugin+'_'+metric+'_follow_dxt.csv'

        check_follow_file(pth_data_files+'/follow/', follow_file)

        start, stop = backup_datetime(pth_data_files + '/follow/' + follow_file)

        datetime_fromat = '%d-%m-%Y %H:%M:%S'
        time_slice_window = {'days': 1}

        start = datetime.datetime.strptime(start, datetime_fromat)
        stop = datetime.datetime.strptime(stop, datetime_fromat)

        if stop - start >= datetime.timedelta(**time_slice_window):
            end = start
            while end < stop:
                #         print(end, stop)
                if start + datetime.timedelta(**time_slice_window) < stop:
                    end = start + datetime.timedelta(**time_slice_window)
                else:
                    end = stop
                logger.info('Backup '+str(start)+' '+str(end))

            #     Do somethings
                try:
                    result = dxt(sq=sq,
                                 tstart=datetime.datetime.strftime(start, '%d-%m-%Y %H:%M:%S'),
                                 tstop= datetime.datetime.strftime(end,   '%d-%m-%Y %H:%M:%S'),
                                 metric=metric)

                    path = pth_data_files + '/'+plugin+'/'+metric+'/'
                    rng = datetime.datetime.strftime(start, '%Y-%m-%d %H:%M:%S').replace(':', '-').replace(
                        ' ', '_') + '_to_'+datetime.datetime.strftime(end, '%Y-%m-%d %H:%M:%S').replace(':', '-').replace(' ', '_')
                    file_name = metric.replace('.', '_')+'_'+rng+'.csv.gz'

                    if result.shape[0] != 0:
                        csv_save_data(result, path, file_name)

                    pd.DataFrame([[datetime.datetime.strftime(end, '%Y-%m-%d %H:%M:%S'), result.shape[0],
                                 result.shape[1]]]).to_csv(pth_data_files + '/follow/' + follow_file, index=False, mode='a', header=False)
                    logger.info(str(plugin)+'_'+str(metric)+' Backup saved until '+str(datetime.datetime.strftime(end, '%Y-%m-%d %H:%M:%S')) + ' Shape ' + str(result.shape))
                except Exception as e:
                    print(e)
                    logger.error('Error'+str(e)+' ==> ' + str(plugin)+'_'+str(metric))

                start = end

        else:
            print(str(plugin)+'_'+str(metric)+' Your dataset already update.')
            logger.info(str(plugin)+'_'+str(metric) + ' Your dataset already update.')

        logger.info(50*'+')





######################
#                    # 
###################### 


now = datetime.datetime.now(pytz.timezone('Europe/Rome'))
pth_data_files = '/scratch/seyedkazemi/Marconi100_backup/'
loggin_path =    "/scratch/seyedkazemi/Marconi100_backup/log/"




check_dir(pth_data_files)
check_dir(loggin_path)

time_for_start_script = 22
processes = 30
number_rows_chunks = 2
metrics_path = 'M100_metrics.csv'
pid = os.getpid()




while True:
    
    if now.hour == time_for_start_script:

        dtx_end_day = datetime.datetime.strftime(now.date(), '%Y-%m-%d')

        with multiprocessing.Pool(processes=processes) as pool:
            pool.map(body_dxt, metrics_chunks(metrics_path=metrics_path, number_rows_chunks=number_rows_chunks))
            

    now = datetime.datetime.now(pytz.timezone('Europe/Rome'))