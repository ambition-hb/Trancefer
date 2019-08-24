#_*_coding:utf-8_*_
import re
import pymysql
import mongodb
import time
import datetime

#建立数据库连接
conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='123456', db='el_shi', charset='utf8')
cur = conn.cursor()
#执行数据库操作
cur.execute("use el_shi")

#模块一：UserInfo
def user_info_table():
    mongo = mongodb.Mongo1()
    items = mongo.db.User_Info.find(no_cursor_timeout=True, batch_size=30)
    num = 1
    for item in items:
        user_ID = str(item.get('user_id'))
        print u"第%d个用户ID：%s" % (num, user_ID)
        num = num + 1
        try:
            answer_count = item.get('answer_count')
        except:
            answer_count = 0
        try:
            ask_count = item.get('ask_count')
        except:
            ask_count = 0
        try:
            following_count = item.get('following_count')
        except:
            following_count = 0
        try:
            follower_count = item.get('follower_count')
        except:
            follower_count = 0
        try:
            topics_count = item.get('topics_count')
        except:
            topics_count = 0
        try:
            achievements = item.get('achievement')
            achievement_num = len(achievements)
        except:
            achievement_num = 0
        if achievement_num != 0:
            for i in range(len(achievements)):
                achievement = achievements[i]
            # print question_ID,follower_ID,follower_name,gender,user_type,answer_count,follower_count,articles_count,follower_num
                cur.execute("insert into user_info(user_ID,ask_count,answer_count,follower_count,following_count,topics_count,achievement,achievement_num) values(%s,%s,%s,%s,%s,%s,%s,%s)",(user_ID,ask_count,answer_count,follower_count,following_count,topics_count,achievement,achievement_num))
                cur.connection.commit()
        else:
            cur.execute("insert into user_info(user_ID,ask_count,answer_count,follower_count,following_count,topics_count,achievement_num) values(%s,%s,%s,%s,%s,%s,%s)",(user_ID,ask_count,answer_count,follower_count,following_count,topics_count,achievement_num))
            cur.connection.commit()
    items.close()

#模块二：UserTopics
def user_topics_table():
    mongo = mongodb.Mongo1()
    items = mongo.db.User_Topics.find(no_cursor_timeout=True, batch_size=30)
    num = 1
    for item in items:
        user_id = str(item.get('user_id'))
        topics_count = item.get('topics_count')
        topic = item.get('topic')
        print u"第%d个用户ID：%s" %(num, user_id)
        num += 1
        if topics_count != 0:
            for i in range(len(topic)):
                contributions_count = topic[i]['contributions_count']
                name = topic[i]['name']
                cur.execute("insert into user_topics(user_ID,name,contributions_count,topics_count) values(%s,%s,%s,%s)",(user_id,name,contributions_count,topics_count))
                cur.connection.commit()
        else:
            cur.execute("insert into user_topics(user_ID,topics_count) values(%s,%s)",(user_id,topics_count))
            cur.connection.commit()

    items.close()

#模块三：UserAnswer
def user_answer_table():
    mongo = mongodb.Mongo1()
    items = mongo.db.User_Answer.find(no_cursor_timeout=True, batch_size=30)
    num = 1
    for item in items:
        user_ID = str(item.get('user_id'))
        print u"第%d个用户ID：%s" % (num, user_ID)
        num = num + 1
        try:
            answer = item.get('answer')
            answer_num = len(answer)
        except:
            answer_num = 0
        if answer_num != 0:
            for i in range(answer_num):
                question_url = answer[i]['question_url']
                question_ID = re.findall("\d+", question_url)[0]
                answer_url = answer[i]['answer_url']
                answer_ID = re.findall("\d+", answer_url)[0]
                answer_content = answer[i]['answer_content']
                create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(answer[i]['created_time'])))
                update_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(answer[i]['updated_time'])))
                try:
                    answer_num = answer[i]['answer_num']
                except:
                    answer_num = 0
                try:
                    comment_count = answer[i]['comment_count']
                except:
                    comment_count = 0
                try:
                    vote_count = answer[i]['vote_count']
                except:
                    vote_count = 0
                topic = answer[i]['belong_topics']
                topic = ",".join(topic)
                cur.execute("insert into user_answer(user_ID,question_ID,answer_ID,answer_content,create_time,update_time,answer_num,comment_count,vote_count,topic) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",(user_ID,question_ID,answer_ID,answer_content,create_time,update_time,answer_num,comment_count,vote_count,topic))
                cur.connection.commit()
        else:
            cur.execute("insert into user_answer(user_ID) values(%s)",(user_ID))
            cur.connection.commit()
    items.close()

#模块四:UserAsk
def user_ask_table():
    mongo = mongodb.Mongo1()
    items = mongo.db.User_Asks.find(no_cursor_timeout=True, batch_size=30)
    num = 1
    for item in items:
        user_ID = str(item.get('user_id'))
        print u"第%d个用户ID：%s" % (num, user_ID)
        num = num + 1
        try:
            ask_count = item.get('asks_count')
            ask_list = item.get('ask_list')
        except:
            ask_count = 0
        if ask_count != 0:
            for i in range(len(ask_list)):
                create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(ask_list[i]['create_time'])))
                update_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(ask_list[i]['update_time'])))
                question_ID = ask_list[i]['question_id']
                ask_content = ask_list[i]['content']
                try:
                    answer_count = ask_list[i]['answer_count']
                except:
                    answer_count = 0
                try:
                    follower_count = ask_list[i]['follower_count']
                except:
                    follower_count = 0
                #跨表寻找该问题所属话题
                topics_list = mongo.db.Ask_Topics.find({"href":"https://www.zhihu.com/question/"+str(question_ID)})

                for topics in topics_list:
                    topic = topics.get('topics')
                    topic = ",".join(topic)
                cur.execute("insert into user_ask(user_ID,ask_count,create_time,update_time,question_ID,ask_content,answer_count,follower_count,topic) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                        (user_ID, ask_count, create_time, update_time, question_ID, ask_content, answer_count,follower_count, topic))
                cur.connection.commit()
        else:
            cur.execute("insert into user_ask(user_ID,ask_count) values(%s,%s)", (user_ID,ask_count))
            cur.connection.commit()
    items.close()
