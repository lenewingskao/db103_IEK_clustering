import os
import json
import time
from datetime import datetime
from pymongo import MongoClient
import jieba
import jieba.analyse
import string


"""
IEK 資料清理並分詞入mongo
"""

if __name__ == "__main__":

    tStart = time.time()  # 計時開始
    start = time.strftime("m-%d %H:%M:%S")

    # 資料去除\xa0 斷行符號
    move = dict.fromkeys((ord(c) for c in u"\xa0\r\n\t\u3000\u00A0"))
    # 分詞去除英文標點符號
    translator = str.maketrans(dict.fromkeys(string.punctuation))

    # 載入字典
    jieba.set_dictionary('./jieba_inc/dict.txt')

    # 讀入停用詞檔
    stopWords = []
    with open('./jieba_inc/stopWords.txt', 'r', encoding='UTF-8') as file:
        for data in file.readlines():
            data = data.strip()
            stopWords.append(data)


    client = MongoClient('localhost', 27017)
    db = client.club
    #db = MongoClient('10.120.28.50', 27017).club

    # 寫入前刪除collect
    db.ieknews.drop()

    seq = 0

    iek_dir = r'C:\DB103\caseClub\crawler_data\IEK_news'
    # os.walk 會走到檔案才停下來
    for dir_path, dir_names, file_names in os.walk(iek_dir):
        for single_file in file_names:
            if not single_file.startswith("."):
                print(single_file)
                with open(os.path.join(dir_path, single_file)) as f:
                    data = json.load(f)
                    # 去除重複新聞
                    if db.ieknews.find_one({"org_title":data.get("newstitle")}):
                        print('continue')
                        continue

                    # 資料清理
                    title = data.get("newstitle").translate(move)
                    news = data.get("news").translate(move)

                    # 分詞 : 使用 jieba.cut(精確模式) -> 去停用字, 數字字詞, 空白字詞
                    t_seq = jieba.lcut(title, cut_all=False)
                    t_cut = list(filter(lambda x: x not in stopWords, t_seq))
                    t_cut2 = list(filter(lambda x: not x.translate(translator).isdigit() and x != ' ', t_cut))
                    n_seg = jieba.lcut(news, cut_all=False)
                    n_cut = list(filter(lambda x: x not in stopWords, n_seg))
                    n_cut2 = list(filter(lambda x: not x.translate(translator).isdigit() and x != ' ', n_cut))

                    # 取關鍵字
                    n_tags = jieba.analyse.extract_tags(" ".join(n_cut2), 20)

                    # 清理日期
                    datestr = data.get("date").split(" ")[0].replace("/","-")
                    ndate = datetime.strptime(datestr, "%Y-%m-%d")

                    # data寫入mongo
                    newjson = {
                        "seq": seq,
                        "org_title": data.get("newstitle"),
                        "org_date": data.get("date"),
                        "org_news": data.get("news"),
                        "year": ndate.year,
                        "month": ndate.month,
                        "day": ndate.day,
                        "title_cut": " ".join(t_cut2),
                        "news_cut": " ".join(n_cut2),
                        "news_kw": " ".join(n_tags)
                    }
                    db.ieknews.insert(newjson)
                    seq += 1

    client.close()

    tEnd = time.time()  # 計時結束
    end = time.strftime("m-%d %H:%M:%S")
    print('共新增', seq ,'筆， 花費:', (tEnd - tStart), 'sec', (tEnd - tStart) // 60, 'min')