# -*- coding: utf-8 -*-
from __future__ import print_function
from __future__ import absolute_import

import datetime
import os, sys
import random
import re
import string
import threading
# from eventlet.green import threading
import time
from urllib.parse import urlparse
# import requests
import eventlet

if sys.version_info.major < 3:
    import Queue
    import ConfigParser as configparser
else:
    import queue as Queue
    import configparser

requests = eventlet.import_patched('requests')


# 预下载，获取m3u8文件，读出ts链接，并写入文档
def down(headers, url, base_url):
    m3u8_dirname = os.path.dirname(url)
    m3u8_urlp = urlparse(url)
    # 当ts文件链接不完整时，需拼凑
    resp = requests.get(url, headers=headers)
    m3u8_text = resp.text
    # print(m3u8_text)
    # 按行拆分m3u8文档
    ts_queue = eventlet.queue.Queue()
    # ts_queue = eventlet.queue.LifoQueue ()
    # ts_queue = Queue(10000)
    lines = m3u8_text.split('\n')
    s = len(lines)
    # 找到文档中含有ts字段的行
    # concatfile = 'cache/' + "s" + '.txt'
    concatfile = 'cache/' + "decode" + '.m3u8'
    if os.path.exists(concatfile):
        os.remove(concatfile)
    s_count = 1
    for i, line in enumerate(lines):
        # if len(line) >=3 and line[-3:] == '.ts':
        if '.ts' in line:
            if 'http' in line:
                # print("ts>>", line)
                http_line = line
                pass
            else:
                path = os.path.dirname(line)
                if len(path) == 0:
                    http_line = m3u8_dirname + '/' + line
                else:
                    http_line = m3u8_urlp.scheme + '://' + m3u8_urlp.netloc + '' + line
                    # line = base_url + line
            # filename = re.search('([a-zA-Z0-9-_]+.ts)', line).group(1).strip()
            # filename = os.path.basename (line)
            filename = str(s_count).zfill(10) + '.ts'
            if not os.path.exists('cache/' + filename):
                # print ("  Add ", filename)
                # ts_queue.put(line)
                ts_queue.put((filename, http_line, 0))
            else:
                # print ("  Had ", filename)
                pass
            # print('ts>>',line)

            # 一定要先写文件，因为线程的下载是无序的，文件无法按照
            # 123456。。。去顺序排序，而文件中的命名也无法保证是按顺序的
            # 这会导致下载的ts文件无序，合并时，就会顺序错误，导致视频有问题。
            # open(concatfile, 'a+').write("file %s\n" % filename)
            open(concatfile, 'a+').write("%s\n" % filename)
            # print("\r", '文件写入中', i, "/", s, end="", flush=True)
            s_count += 1
            print("\r", '写入中', s_count, "/", s, http_line, end="", flush=True)
        else:
            # 若发现了 加密 key，则把 key 本地化
            key_re = re.search("(URI=\".*\.key\")", line)
            if key_re != None:
                key_url = key_re.group(1).strip()
                key_url = key_url[5:-1]
                path = os.path.dirname(key_url)
                if len(path) == 0:
                    http_key = m3u8_dirname + '/' + key_url
                else:
                    http_key = m3u8_urlp.scheme + '://' + m3u8_urlp.netloc + '' + key_url

                key_line = line[:key_re.start() + 5] + "key.key" + line[key_re.end() - 1:]
                print(line, key_url, http_key, key_line, "\n")

                key_r = requests.get(http_key, stream=True, headers=headers, timeout=(15, 60), verify=True)
                with open('cache/key.key', 'wb') as fp:
                    for chunk in key_r.iter_content(5242):
                        if chunk:
                            fp.write(chunk)
                open(concatfile, 'a+').write(key_line + "\n")
            else:
                open(concatfile, 'a+').write(line + "\n")
    return ts_queue, concatfile


# 线程模式，执行线程下载
def run(ts_queue, headers, pool):
    while True:
        try:
            # url, sleepTime = ts_queue.get (True, 0.5)
            filename, url, sleepTime = ts_queue.get(True, 0.5)
        except Queue.Empty:
            break

        if sleepTime > 0:
            eventlet.sleep(sleepTime)
        # filename = re.search('([a-zA-Z0-9-_]+.ts)', url).group(1).strip()
        # filename = os.path.basename (url)
        requests.packages.urllib3.disable_warnings()

        try:
            r = requests.get(url, stream=True, headers=headers, timeout=(15, 60), verify=False)
            r.raise_for_status()
            with open('cache/' + filename, 'wb') as fp:
                for chunk in r.iter_content(5242):
                    if chunk:
                        fp.write(chunk)
            print("\r", '任务文件 ', filename, ' 下载成功', pool.running(), ts_queue.qsize(), end="         ", flush=True)
        except Exception as exc:
            print('任务文件 ', filename, ' 下载失败, 代码:', exc)
            ts_queue.put((filename, url, 5))
            # eventlet.sleep (2)
    # return True


# 视频合并方法，使用ffmpeg
def merge(concatfile, name):
    try:
        # path = 'cache/' + name + '.mp4'
        path =  name + '.mp4'
        # command = 'ffmpeg -y -f concat -i %s -crf 18 -ar 48000 -vcodec libx264 -c:a aac -r 25 -g 25 -keyint_min 25 -strict -2 %s' % (concatfile, path)
        command = "ffmpeg -allowed_extensions ALL -protocol_whitelist \"file,http,crypto,tcp\" "
        # command += ' -y -f concat -i %s -bsf:a aac_adtstoasc -c copy %s' % (concatfile, path)
        command += ' -y -i %s -bsf:a aac_adtstoasc -c copy %s' % (concatfile, path)
        print(command)
        os.system(command)
        print('视频合并完成')
    except:
        print('合并失败')


def remove():
    dir = 'cache/'
    """
    #for line in open('cache/s.txt'):
    for line in open('cache/decode.m3u8'):
        #line = re.search('file (.*?ts)', line).group(1).strip()
        line = re.search('(.*?ts)', line).group(1).strip()
        # print(line)
        os.remove(dir + line)
    print("ts文件全部删除")
    try:
        os.remove('cache/s.txt')
        print('文件删除成功')
    except:
        print('文件删除失败')
    """
    command = "del " + dir + "*/Q"
    os.system(command)


# headers 和 base_url 必须根据实际网站 ！手动 ！ 设置
if __name__ == '__main__':
    # 测试用链接：https://yiyi.55zuiday.com/ppvod/70B5A6E3A150A99882E28EC793CAF519.m3u8
    # 链接电影：地球最后的夜晚
    # https://youku.com-ok-sohu.com/20191110/20128_fd24c5a9/1000k/hls/61033a1fdc2000000.ts
    # base_url = 'https://yiyi.55zuiday.com/'
    base_url = 'https://yiyi.55zuiday.com/ppvod/70B5A6E3A150A99882E28EC793CAF519.m3u8'
    headers = {
        # 'referer': 'https://yiyi.55zuiday.com/share/wVuAcJFy1tMy4t0x',
        'referer': 'http://www.douying99.com/play/47309_m3u8_0.html',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36'
    }

    requests.adapters.DEFAULT_RETRIES = 5
    today = time.strftime("%Y_%m_%d_%H_%M_%S", time.localtime())

    ran_str = ''.join(random.sample(string.ascii_letters + string.digits, 8))
    print(ran_str)
    name = today + "_" + ran_str
    # headers['referer'] = input('请输入网页链接：').strip()
    url = input('请输入视频m3u8链接：').strip()

    start = datetime.datetime.now().replace(microsecond=0)
    print("目录文件开始写入")
    s, concatfile = down(headers, url, base_url)
    print('\n')
    print("目录文件写入结束")
    # 获取队列元素数量
    t_num = s.qsize()
    # 根据数量来开线程数，每五个元素一个线程
    # 最大开到50个
    print("下载任务开始")
    """
    if num > 5:
        t_num = num // 5
    else:
        t_num = 1
    """
    if t_num > 60:
        t_num = 60
    # print(s,concatfile)

    pool = eventlet.GreenPool(t_num)
    run_args = {'ts_queue': s, 'headers': headers, 'pool': pool}
    for i in range(t_num):
        pool.spawn_n(run, **run_args)
    pool.waitall()
    """
    threads = []
    for i in range(t_num):
        t = threading.Thread(target=run, name='th-' + str(i), kwargs={'ts_queue': s, 'headers': headers})
        t.setDaemon(True)
        threads.append(t)
    for t in threads:
        time.sleep(0.4)
        t.start()
    for t in threads:
        t.join()
    """
    print('\n')
    print("下载任务结束")
    end = datetime.datetime.now().replace(microsecond=0)
    print('写文件及下载耗时：' + str(end - start))
    merge(concatfile, name)
    remove()
    over = datetime.datetime.now().replace(microsecond=0)
    print('合并及删除文件耗时：' + str(over - end))
    print("所有任务结束 ", name)
    print('任务总时长：', over - start)