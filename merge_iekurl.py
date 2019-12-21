import os
import json
import time

# connect mysql db
from cr_1111.myConnect import myConnect

"""
IEK URL 合併回 mysql ieknews
"""

if __name__ == "__main__":

    cnt = 0
    # mysql connect
    mydb = myConnect()

    urldir = r'E:\專題\crawler_data\iek_url'
    # os.walk 會走到檔案才停下來
    for dir_path, dir_names, file_names in os.walk(urldir):
        for single_file in file_names:
            if not single_file.startswith("."):
                # print(single_file)
                with open(os.path.join(dir_path, single_file)) as f:
                    data = json.load(f)
                    newstitle = data.get("newstitle").strip()
                    newsurl =  data.get("url").strip()

                    iekseq = mydb.queryone("select seq from ieknews where otitle=%s", newstitle)
                    if iekseq:
                        sql = "update ieknews set url=%s where seq=%s"
                        mydb.execmmit(sql, (newsurl, iekseq[0]))
                        cnt += 1
                        print(iekseq, newstitle, newsurl)
                        #print(type(newsurl), type(iekseq))

    # mysql disconnect
    mydb.close()

    print('-------------- Done -------------------')
    print(cnt)