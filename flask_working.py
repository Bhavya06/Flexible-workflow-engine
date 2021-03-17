from flask import Flask, make_response, request, render_template, jsonify, Response, redirect,url_for, stream_with_context
import time
import json
from json import JSONEncoder
from time import sleep
from multiprocessing import Process, Manager
import threading
import random
import concurrent.futures
from werkzeug.utils import secure_filename
import nltk
nltk.download('stopwords')
import string
import multiprocessing
import pandas as pd
from flask_cors import CORS, cross_origin
from IPython.display import HTML
import numpy as np

app = Flask(__name__,static_url_path="/",static_folder='C:/User/USER/Desktop/proj', template_folder='templates')

CORS(app, support_credentials=True)

class Engine(object):
    def __init__(self, name, start_node, current_node, total_time,last_node, priority_users, is_executing):
        self.name = name
        self.start_node = start_node
        self.current_node = current_node
        self.total_time = total_time
        self.last_node = last_node
        self.priority_users = priority_users
        self.is_executing = is_executing

class Link(object):
    def __init__(self, id, fromOperator, fromConnector, toOperator, toConnector):
        self.id = id
        self.fromOperator = fromOperator
        self.fromConnector = fromConnector
        self.toOperator = toOperator
        self.toConnector = toConnector

class Operator(object):
    def __init__(self, id, title, time, input, output, not_executed, fin_executed, jobs, is_executing, executing_user, wait_time):
        self.id = id
        self.title = title
        self.time = time
        self.input = input
        self.output = output
        self.not_executed = not_executed
        self.fin_executed = fin_executed
        self.jobs = jobs
        self.is_executing = is_executing
        self.executing_user = executing_user
        self.wait_time = wait_time

global result
result = {}
result['operators'] = {}
result['links'] = {}
global operator_data
global operatorI
operatorI = 0
operator_data={}
global time_list
time_list =[]
taskpostop = 0
taskposleft = 0
global user_no

app.config['UPLOAD_FOLDER'] = 'C:/User/USER/Desktop/proj'
app.config['MAX_CONTENT_PATH'] = 1000000


fileop = open("Display_logs.txt", "w")
fileop.close()
@app.route('/writetofile', methods = ['POST'])
def writetofile():
    fileop = open("Display_logs.txt", "a")
    jsdata = request.form['javascript_data']
    fileop.write(jsdata)
    fileop.write("\n")
    fileop.close()
    return jsdata

@app.route('/read_file', methods = ["POST"])
def Submit():
    f = open("Tasks1.txt")
    c=0
    task_data =""
    link_data = ""
    user_data =""
    for x in f:
        # print(x)
        x = x.rstrip('\n')
        r = x.split()
        if(c==0):
            workflow_name = x
            c=1
        elif(r[0] == 'TASKS'):
            task_data = ""
        elif(r[0] == 'LINK'):
            link_data = ""
        elif(r[0] == 'USER'):
            user_data = int(r[1])
        elif(r[-1][0].isdigit()):
            task_data+= x
            task_data+= " "
        elif(r[-1][0].isalpha()):
            link_data+= x
            link_data+= " "
        

    task_data = task_data[0:len(task_data)-1]
    link_data = link_data[0:len(link_data)-1]

    val = {}
    val["td"] = str(task_data)
    val["ld"] = str(link_data)
    val["wn"] = str(workflow_name)
    val["un"] = str(user_data)

    resp = jsonify(val)
    resp.headers['Access-Control-Allow-Origin']='*'
    return resp
    	
from nltk.corpus import stopwords
stopwords = list(stopwords.words('english'))
stopwords.extend(list(string.punctuation))  
stopwords.append("i\'ve")
stopwords.append("i\'m")
stopwords.remove("no")
stopwords.remove("not")
stopwords.remove("than")
stopwords.remove("which")
stopwords.remove("or")

def remove_stopwords(text):
    """custom function to remove the stopwords"""
    return " ".join([word for word in str(text).split() if word not in stopwords])

@app.route('/uploader', methods = ['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        f = request.files['file']
        f.save(secure_filename(f.filename))
        #f1 = open(f.filename , 'r')
        #a = f1.read()
        flist = open(f.filename).readlines()
        a = ''
        for s in flist:
            a = a + ' ' + s.rstrip('\n')
        #print("!!!!!!!!!!!!", a)
        b = remove_stopwords(a.lower())
        b = b.replace("sec",'')
        #print(b)
        workflow_name = ''
        global tasks
        tasks = []
        time1 = []
        link = []
        i=0
        prev = []
        temp = []
        or_flag = 0
        or_pos = -1
        global user_no
        user_no = 0
        while(i<len(b)):
            if(b[i] == "@"):
                ind = b[i:].index(":")
                word = b[i+1:ind+i]
                if(word == 'workflow'):
                    fs = b[i+ind+1:].index('.')
                    workflow_name = b[ind+2+i:i+ind+1+fs]
                elif(word == 'user'):
                    fs = b[i+ind+1:].index('.')
                    user_no = b[ind+2+i-1:i+ind+1+fs]
                    print("*************%",user_no)
                elif(word=='task'):
                    at = b[i+ind+1:].index('@')
                    tasks.append(b[i+ind+2:at+ind+i])
                    fs = b[ind+1+i:].index('.')
                    if('or' in  b[ind+1+i:]):
                        or_pos = b[ind+1+i:].index('or')
                    else:
                        or_pos = -1
                    if(or_pos!=-1):
                        if(fs < or_pos):
                            colon = b[ind+1+i:].index(':')
                            time1.append(b[colon+i+3+ind:i+ind+fs])
                        else:
                            colon = b[ind+1+i:].index(':')
                            time1.append(b[colon+i+3+ind:i+ind+or_pos-1])
                    else:
                        colon = b[ind+1+i:].index(':')
                        time1.append(b[colon+i+3+ind:i+ind+fs])
                    if(temp == []):
                        temp = [tasks[-1]]
                    else:
                        if(or_pos!= -1 and or_pos < fs):
                            prev = temp.copy()
                            temp = [tasks[-1]]
                            link.append([prev[0],temp[-1]])
                            or_flag = 1
                        else:
                            if(or_flag == 0):
                                prev = temp.copy()
                                temp= [tasks[-1]]
                                if(len(prev) == 1):
                                    link.append([prev[0],temp[-1]])
                                else:
                                    for k in range(0,len(prev)):
                                        link.append([prev[k],temp[-1]])
                                        #print('$$$$$$',link)
                            else:
                                temp.append(tasks[-1])
                                link.append([prev[0],temp[-1]])
                                or_flag = 0
                        # print('******', prev)
                        # print("*****",temp)
                if(or_pos!=-1 and or_pos<fs):
                    i=or_pos+1+ind+1+i
                else:
                    i=fs+1+ind+1+i
            else:
                i=i+1
        # print(workflow_name)
        # print(tasks)
        # print(time)
        # print(link)
        global time_list
        time_list = time1
        workflow_name = workflow_name[0].upper() + workflow_name[1:]
        f = open('Tasks1.txt', "w")
        f.write(workflow_name+'\n')
        f.write('TASKS\n')
        for i in range(len(tasks)):
            f.write(tasks[i] +' '+ time1[i]+'\n')
        f.write('LINK\n')
        for i in link:
            f.write(i[0] + ' '+i[1]+'\n')
        f.write('USER ')
        f.write(str(user_no)+'\n')
    return render_template('demo.html')

@app.route("/start",methods=["GET"])
def start():
    workflow_name = "TEST"
    globals()[workflow_name] = Engine(workflow_name, 'NULL', 'NULL', 0, 'NULL', [], False)
    myeng = eval(workflow_name)
    global result
    result ={}
    result['operators'] = {}
    result['links'] = {}
    return render_template('demo.html',  data=result)

@app.route("/add_operator",methods=["POST"])
@cross_origin(supports_credentials=True)
def add_operator():
    workflow_name = "TEST"
    myeng = eval(workflow_name)
    global result
    global operatorI
    global operator_data
    global taskpostop
    global taskposleft
    operatorId = 'created_operator_' + str(operatorI)
    operatorData = {
        "top": 130,
        "left": 20 + taskposleft,
        "properties": {
            "title": 'Task' + str(operatorI + 1),
            "inputs": {
                "input_1": {
        "left": (800 / 2) - 100 + (operatorI * 10),
                    "label": 'Input 1',
                }
            },
            "outputs": {
                "output_1": {
                    "label": 'Output 1',
                }
            }
        }
    }       
    taskpostop = taskpostop + 50
    taskposleft = taskposleft + 220
    task_name = operatorId
    time = 0
    globals()[task_name] = Operator(task_name,'Task' + str(operatorI + 1) + '[ Time: 0 ]', time,[], [],[],[],0, False, 'NULL',0)      
    if(myeng.start_node == 'NULL'):
        myeng.start_node = eval(task_name) 
        myeng.last_node = eval(task_name)
    #print("FIRST NODE LAST NODE", myeng.start_node.title, myeng.last_node.title)
    result['operators'][operatorId] = operatorData
    operatorID={}
    operatorID['name'] = operatorId
    operator_data["operatorId"] = operatorID
    operator_data["operatorData"] = operatorData
    resp = jsonify(operator_data)
    operatorI=operatorI + 1
    #print(result['operators'].keys())
    resp.headers['Access-Control-Allow-Origin']='*'
    return resp

@app.route("/add_link",methods=["POST"])
@cross_origin(supports_credentials=True)
def add_link():
    workflow_name = "TEST"
    myeng = eval(workflow_name)
    global result
    rf=request.form
    for key in rf.keys():
        data=key
    data_dic=json.loads(data)
    linkId = 'link'+str(data_dic['linkId'])
    linkDataOriginal = data_dic['linkDataOriginal']
    result['links'][linkId]= linkDataOriginal
    link_name = linkId
    not_executed = []
    if(myeng.is_executing == True):
        if(eval(linkDataOriginal['fromOperator']).executing_user != 'NULL'):
            not_executed.append(eval(linkDataOriginal['fromOperator']).executing_user)
            eval(linkDataOriginal['toOperator']).jobs+=1
        for i in eval(linkDataOriginal['fromOperator']).not_executed:
            not_executed.append(i)
            eval(linkDataOriginal['toOperator']).jobs+=1
        eval(linkDataOriginal["toOperator"]).not_executed = not_executed
        tasks_to_do.append(eval(linkDataOriginal["toOperator"]))
        print('TASKS TO DO', tasks_to_do)
    #print(eval(linkDataOriginal["toOperator"]).not_executed, eval(linkDataOriginal['toOperator']).jobs )
    globals()[link_name] = Link(link_name,eval(linkDataOriginal['fromOperator']), linkDataOriginal['fromConnector'],eval(linkDataOriginal['toOperator']), linkDataOriginal['toConnector'])
    # print("&&&&&&&&&&&&From operator", linkDataOriginal['fromOperator'], "To operator", linkDataOriginal["toOperator"])
    task_name  = linkDataOriginal['fromOperator']
    # print("&&&&&&&&&&&&From operator", eval(task_name).output)
    eval(task_name).output.append(eval(link_name))
    # print("&&&&&&&&&&&&From operator", eval(task_name).output)
    #print(eval(task_name).output, eval(task_name).title)
    task_name  = linkDataOriginal['toOperator']
    # print("&&&&&&&&&&&&To operator", eval(task_name).input)
    eval(task_name).input.append(eval(link_name))
    # print("&&&&&&&&&&&&To operator", eval(task_name).input)
    #print(eval(task_name).input, eval(task_name).title)
    if(eval(linkDataOriginal['fromOperator']) == myeng.last_node):
        myeng.last_node = eval(linkDataOriginal["toOperator"])
    #print("FIRST NODE LAST NODE", myeng.start_node.title, myeng.last_node.title)

    resp_dic={}
    resp = jsonify(resp_dic)
    #print(result['links'].keys())
    resp.headers['Access-Control-Allow-Origin']='*'
    return resp
    
@app.route("/change_title",methods=["POST"])
def change_title():
    global result
    rf=request.form
    for key in rf.keys():
        data=key
    data_dic=json.loads(data)
    operatorId = data_dic['operatorId']
    newTitle = data_dic['newTitle']
    if(data_dic['time'] !=''):
        time = int(data_dic['time'])
    else:
        time = 0
    result['operators'][operatorId]['properties']['title'] = newTitle
    task_name = operatorId
    globals()[task_name].title = newTitle
    globals()[task_name].time = time
    resp_dic={}
    resp = jsonify(resp_dic)
    resp.headers['Access-Control-Allow-Origin']='*'
    return resp

@app.route("/add_priority_user",methods=["POST"])
def add_priority_user():
    #'User3'
    workflow_name = "TEST"
    myeng = eval(workflow_name)
    rf=request.form
    for key in rf.keys():
        data=key
    data_dic=json.loads(data)
    priorityUser = data_dic['priorityUser']
    myeng.priority_users.append(priorityUser)
    resp_dic={}
    resp = jsonify(resp_dic)
    resp.headers['Access-Control-Allow-Origin']='*'
    return resp
   
@app.route("/query1",methods=["POST"])
def query1():
    rf=request.form
    for key in rf.keys():
        data=key
    data_dic=json.loads(data)
    no_of_users = data_dic['no_of_users']
    global time_list
    l = time_list.copy()
    l.insert(0,0)#l=[0,5,7,4,2]
    #print(':::',no_of_users)
    n = int(float(no_of_users))
    executing_tasks = [1]
    for i in range(len(l)):
        l[i] = int(l[i])
    wait_time = [l[1]]
    current_user = []#
    queue = []
    finished_users = 0
    total_time = 0
    for i in range(len(l)):
        queue.append([])
        if(i==1):
            current_user.append(1)
        else:
            current_user.append(0)
    for i in range(2,n+1):
        queue[1].append(i)
    #print('QUERY1',queue, current_user, l, executing_tasks, wait_time)
    global tasks
    
    val = {}
    val['User1'] = {}
    val['User1'][tasks[0]] = [0]
    #print('User1 Started ' + tasks[0]+ ' 0')
    while(executing_tasks):
        m = min(wait_time)
        #print(m)
        total_time = total_time+m
        i=0
        while(i<len(executing_tasks)):
            wait_time[i] = wait_time[i] - m
            task = executing_tasks[i]
            new_temp = {}
            flag = 0
            if(wait_time[i]==0):
                #print('User'+ str(current_user[task])+' Finished '+tasks[task-1]+' '+ str(total_time))
                #print(val)
                val['User'+ str(current_user[task])][tasks[task-1]].append(total_time)
                if(task!=len(l)-1):
                    if(current_user[task+1]==0):
                        current_user[task+1] = current_user[task]
                        executing_tasks.insert(i+1, task+1)
                        wait_time.insert(i+1, l[task+1])
                        flag = 1
                        i=i+1
                        #print('User'+ str(current_user[task+1])+' Started '+tasks[task]+' '+ str(total_time))
                        #print(val)
                        if('User'+ str(current_user[task]) not in val):
                            val['User'+ str(current_user[task])] = {}
                        val['User'+ str(current_user[task+1])][tasks[task]] = [total_time]
                    else:
                        queue[task+1].append(current_user[task])
                    
                else:
                    finished_users +=1
                if(queue[task] !=[]):
                    new_user = queue[task].pop(0)
                    current_user[task] = new_user
                    #print('User'+str(current_user[task])+' Started ' +tasks[task-1] +'  '+ str(total_time))
                    #print(val)
                    if('User'+ str(current_user[task]) not in val):
                        val['User'+ str(current_user[task])] = {}
                    if(tasks[task-1] not in val['User'+ str(current_user[task])]):
                        val['User'+ str(current_user[task])][tasks[task-1]] = {}
                    val['User'+ str(current_user[task])][tasks[task-1]] = [total_time]
                    if(flag == 1):
                        wait_time[i-1] = l[task]
                    else:
                        wait_time[i] = l[task]
                    i+=1
                else:
                    current_user[task] = 0
                    if(flag == 1):
                        wait_time.pop(i-1)
                        executing_tasks.pop(i-1)
                    else:
                        wait_time.pop(i)
                        executing_tasks.pop(i)
            else:
                i+=1
        # print('min',m)
        # print('***',executing_tasks,'\t', wait_time)
        # print('$$$',current_user,'\t',queue)
    # print(total_time)
    # print(val)
    val1 = {}
    df = pd.DataFrame.from_dict(val)
    print(df)
    ls = df.values.tolist()
    tasknames = list(val["User1"].keys())
    for i in range(0,len(ls)):
        ls[i].insert(0,tasknames[i])

    usernames = list(val.keys())
    usernames.insert(0,"Tasknames")

    print(len(usernames))
    print(" ------ ")
    print(ls, len(ls[0]))



    # print(df.to_html())
    # HTML(df.to_html(classes='table table-striped'))
    # val1['query1_details'] = df.to_html()
    val1["total_time"] = str(total_time)
    val1["tabledata"] = ls
    val1["row"] = len(ls)
    val1["column"] = len(ls[0])
    val1["tableheader"] = usernames
    resp = jsonify(val1)
    resp.headers['Access-Control-Allow-Origin']='*'
    return resp
    # return render_template('demo.html',tables=[df.to_html(classes='data')],titles=['title'])


def query2_table(no):
    global time_list
    l = time_list.copy()
    l.insert(0,0)#l=[0,5,7,4,2]
    #print(':::',no_of_users)
    n = no
    executing_tasks = [1]
    for i in range(len(l)):
        l[i] = int(l[i])
    wait_time = [l[1]]
    current_user = []#
    queue = []
    finished_users = 0
    total_time = 0
    for i in range(len(l)):
        queue.append([])
        if(i==1):
            current_user.append(1)
        else:
            current_user.append(0)
    for i in range(2,n+1):
        queue[1].append(i)
    #print('QUERY1',queue, current_user, l, executing_tasks, wait_time)
    global tasks
    
    val = {}
    val['User1'] = {}
    val['User1'][tasks[0]] = [0]
    #print('User1 Started ' + tasks[0]+ ' 0')
    while(executing_tasks):
        m = min(wait_time)
        #print(m)
        total_time = total_time+m
        i=0
        while(i<len(executing_tasks)):
            wait_time[i] = wait_time[i] - m
            task = executing_tasks[i]
            new_temp = {}
            flag = 0
            if(wait_time[i]==0):
                #print('User'+ str(current_user[task])+' Finished '+tasks[task-1]+' '+ str(total_time))
                #print(val)
                val['User'+ str(current_user[task])][tasks[task-1]].append(total_time)
                if(task!=len(l)-1):
                    if(current_user[task+1]==0):
                        current_user[task+1] = current_user[task]
                        executing_tasks.insert(i+1, task+1)
                        wait_time.insert(i+1, l[task+1])
                        flag = 1
                        i=i+1
                        #print('User'+ str(current_user[task+1])+' Started '+tasks[task]+' '+ str(total_time))
                        #print(val)
                        if('User'+ str(current_user[task]) not in val):
                            val['User'+ str(current_user[task])] = {}
                        val['User'+ str(current_user[task+1])][tasks[task]] = [total_time]
                    else:
                        queue[task+1].append(current_user[task])
                    
                else:
                    finished_users +=1
                if(queue[task] !=[]):
                    new_user = queue[task].pop(0)
                    current_user[task] = new_user
                    #print('User'+str(current_user[task])+' Started ' +tasks[task-1] +'  '+ str(total_time))
                    #print(val)
                    if('User'+ str(current_user[task]) not in val):
                        val['User'+ str(current_user[task])] = {}
                    if(tasks[task-1] not in val['User'+ str(current_user[task])]):
                        val['User'+ str(current_user[task])][tasks[task-1]] = {}
                    val['User'+ str(current_user[task])][tasks[task-1]] = [total_time]
                    if(flag == 1):
                        wait_time[i-1] = l[task]
                    else:
                        wait_time[i] = l[task]
                    i+=1
                else:
                    current_user[task] = 0
                    if(flag == 1):
                        wait_time.pop(i-1)
                        executing_tasks.pop(i-1)
                    else:
                        wait_time.pop(i)
                        executing_tasks.pop(i)
            else:
                i+=1
        # print('min',m)
        # print('***',executing_tasks,'\t', wait_time)
        # print('$$$',current_user,'\t',queue)
    # print(total_time)
    # print(val)
    val1 = {}
    df = pd.DataFrame.from_dict(val)
    ls = df.values.tolist()
    tasknames = list(val["User1"].keys())
    for i in range(0,len(ls)):
        ls[i].insert(0,tasknames[i])

    usernames = list(val.keys())
    usernames.insert(0,"Tasknames")

    val1["tabledata"] = ls
    val1["row"] = len(ls)
    val1["column"] = len(ls[0])
    val1["tableheader"] = usernames
    print(df)

    return val1
    

@app.route("/query2",methods=["POST"])
def query2():
    rf=request.form
    for key in rf.keys():
        data=key
    data_dic=json.loads(data)
    time = data_dic['total_time']
    global time_list
    l = time_list.copy()
    l.insert(0,0)#l=[0,5,7,4,2]
    n = int(float(time))
    executing_tasks = [1]
    for i in range(len(l)):
        l[i] = int(l[i])
    wait_time = [l[1]]
    current_user = []
    queue = []
    finished_users = 0
    add_user = 2
    total_time = 0
    for i in range(len(l)):
        queue.append([])
        if(i==1):
            current_user.append(1)
        else:
            current_user.append(0)
    print(current_user, queue, wait_time, executing_tasks)
    #print('User1 Started ' + tasks[0]+ ' 0')
    while(executing_tasks and total_time < n):
        m = min(wait_time)
        total_time = total_time+m
        i=0
        while(i<len(executing_tasks)):
            wait_time[i] = wait_time[i] - m
            task = executing_tasks[i]
            new_temp = {}
            flag = 0
            if(wait_time[i]==0):
                if(task!=(len(l)-1)):
                    if(current_user[task+1]==0):
                        current_user[task+1] = current_user[task]
                        executing_tasks.insert(i+1, task+1)
                        wait_time.insert(i+1, l[task+1])
                        flag = 1
                        i=i+1
                    else:
                        queue[task+1].append(current_user[task])	
                else:
                    finished_users +=1
                if(task!=1):
                    if(queue[task] !=[]):
                        new_user = queue[task].pop(0)
                        current_user[task] = new_user
                        if(flag == 1):
                            wait_time[i-1] = l[task]
                        else:
                            wait_time[i] = l[task]
                        i+=1
                    else:
                        current_user[task] = 0
                        if(flag == 1):
                            wait_time.pop(i-1)
                            executing_tasks.pop(i-1)
                        else:
                            wait_time.pop(i)
                            executing_tasks.pop(i)
                else:
                    new_user = add_user
                    add_user+=1
                    current_user[task] = new_user
                    if(flag == 1):
                        wait_time[i-1] = l[task]
                    else:
                        wait_time[i] = l[task]
                    i+=1           
            else:
                i+=1
    print(n, total_time)
    if(total_time > n):
        finished_users = finished_users-1
    print(finished_users)
    val1 = {}
    print("QUERY 2")
    val1 = query2_table(finished_users)
    val1["finished_users"] = str(finished_users)
    print(val1)
    resp = jsonify(val1)

    resp.headers['Access-Control-Allow-Origin']='*'
    
    return resp

@app.route("/delete_operator",methods=["DELETE"])
def delete_operator():
    workflow_name = "TEST"
    myeng = eval(workflow_name)
    global result
    rf=request.form
    for key in rf.keys():
        data=key
    data_dic=json.loads(data)
    operatorId = data_dic['operatorId']
    #print(eval(operatorId))
    if(myeng.start_node == eval(operatorId)):
        if(eval(operatorId).output!=[]):
            myeng.start_node = eval(operatorId).output[0].toOperator
        else:
            myeng.start_node = 'NULL'

    if(myeng.last_node == eval(operatorId)):
        if(eval(operatorId).output!=[]):
            myeng.last_node = eval(operatorId).input[0].toOperator
        else:
            myeng.last_node = 'NULL'

    if(eval(operatorId).input!=[]):
        for i in range(eval(operatorId).input):
            eval(operatorId).input[i].toOperator='NULL'
            eval(operatorId).input[i].toConnector = 'NULL'
            # del eval(operatorId).input[i]
    if(eval(operatorId).output!=[]):
        for i in range(eval(operatorId).output):
            eval(operatorId).output[i].fromOperator='NULL'
            eval(operatorId).output[i].fromConnector = 'NULL'  
            # del eval(operatorId).output[i] 
    del result['operators'][operatorId] 
    del globals()[operatorId]
    resp_dic={}
    resp = jsonify(resp_dic)
    #print(result['operators'].keys())
    resp.headers['Access-Control-Allow-Origin']='*'
    return resp

@app.route("/delete_link",methods=["DELETE"])
def delete_link():
    global result
    rf=request.form
    for key in rf.keys():
        data=key
    data_dic=json.loads(data)
    linkId = 'link'+str(data_dic['linkId'])
    if(globals()[linkId].toOperator!='NULL'):
        globals()[linkId].toOperator.input.remove(eval(linkId))
    if(globals()[linkId].fromOperator!='NULL'):
        globals()[linkId].fromOperator.output.remove(eval(linkId))
    del result['links'][linkId] 
    del globals()[linkId]
    resp_dic={}
    resp = jsonify(resp_dic)
    #print(result['links'].keys())
    resp.headers['Access-Control-Allow-Origin']='*'
    return resp

global val
val =''
global user_count 
user_count = 1

def execute_node(current_node):
    #print("hello")
    print("***********************IN EXECUTE NODE",current_node.title,current_node.is_executing, current_node.not_executed)
    # print("----------", task)
    workflow_name = "TEST"
    myeng = eval(workflow_name)
    if(current_node.is_executing == False):
        prev_link = current_node.input
        # if(prev_link=='NULL'):
        #     print("CURRENT   LINK",current_node.title,prev_link)
        # else:
        #     print("CURRENT   LINK",current_node.title,prev_link.id,current_node.not_executed)
        if(prev_link == []):
            #print("IN IF")
            while(current_node.jobs>0 and current_node.not_executed != [] and current_node.is_executing==False):
                if(myeng.last_node.is_executing==True or myeng.last_node.not_executed!=[]):
                    myeng.is_executing=True
                else:
                    myeng.is_executing=False
                not_exec = current_node.not_executed.pop(0)
                current_node.executing_user = not_exec
                current_node.is_executing = True
                val = "Started execution of" +" "+current_node.title+" for "+not_exec+ "\n"
                yield "data: %s\n\n" % (val)
                time.sleep(0.1)
                print("Started execution of" +" "+current_node.title+" for "+not_exec)
                time.sleep(current_node.time)
                val = "Finished execution of" +" "+current_node.title+" for "+not_exec+ "\n"
                yield "data: %s\n\n" % (val)
                time.sleep(0.1)
                print("Finished execution of" +" "+current_node.title+" for "+not_exec)
                current_node.executing_user = 'NULL'
                current_node.fin_executed.append(not_exec)
                current_node.jobs-=1 
                current_node.is_executing = False
        else:
            #print("IN ELSE")
            # prev_link = prev_link[0]
            # prev_node = prev_link.fromOperator
            c=0
            ans = False
            while(current_node.not_executed != [] and ans == False ):
                # and current_node.not_executed[0] not in prev_node.fin_executed
                for i in prev_link:
                    if(current_node.not_executed != []):
                        ans = ans or current_node.not_executed[0] in i.fromOperator.fin_executed
                c+=1
            while(current_node.jobs>0 and current_node.not_executed != [] and current_node.is_executing==False and ans==True):
                if(myeng.last_node.is_executing==True or myeng.last_node.not_executed!=[]):
                    myeng.is_executing=True
                else:
                    myeng.is_executing=False
                not_exec = current_node.not_executed.pop(0)
                current_node.executing_user = not_exec
                current_node.is_executing = True
                random_no = random.randint(1,3)
                #print(random_no)
                # if(random_no == 1):
                #     random_delay = random.randint(2,7)
                #     current_node.wait_time += random_delay
                #     val = "STARTED DELAY of "+str(random_delay)+" sec for "+not_exec+ "\n"
                #     yield "data: %s\n\n" % (val)
                #     time.sleep(0.1)
                #     print("STARTED DELAY of "+str(random_delay)+" sec for "+not_exec)
                #     time.sleep(random_delay)
                #     val = "FINISHED DELAY of "+str(random_delay)+" sec for "+ not_exec+ "\n"
                #     yield "data: %s\n\n" % (val)
                #     time.sleep(0.1)
                #     print("FINISHED DELAY of "+str(random_delay)+" sec for "+not_exec)
                #     current_node.wait_time -= random_delay
                
                val = "Started execution of" +" "+current_node.title+" for "+not_exec+ "\n"
                yield "data: %s\n\n" % (val)
                time.sleep(0.1)
                print("Started execution of" +" "+current_node.title+" for "+not_exec)
                time.sleep(current_node.time)
                val = "Finished execution of" +" "+current_node.title+" for "+not_exec+ "\n"
                yield "data: %s\n\n" % (val)
                time.sleep(0.1)
                print("Finished execution of" +" "+current_node.title+" for "+not_exec)
                current_node.fin_executed.append(not_exec)
                current_node.executing_user = 'NULL'
                current_node.jobs-=1 
                current_node.is_executing = False
                current_node.wait_time -= current_node.time
                #print(current_node.title,current_node.is_executing)
                ans = False
                while(current_node.not_executed != [] and ans == False ):
                    # and current_node.not_executed[0] not in prev_node.fin_executed
                    for i in prev_link:
                        if(current_node.not_executed != []):
                            ans = ans or current_node.not_executed[0] in i.fromOperator.fin_executed
                    print(ans)
        # current_node.is_executing = False
        if(myeng.last_node.is_executing==True or myeng.last_node.not_executed!=[]):
            myeng.is_executing=True
        else:
            myeng.is_executing=False
    
global tasks_to_do
tasks_to_do = []
global tasks
tasks = []

@app.route("/execute_engine",methods=["GET"])  
def execute_engine(): 
    if request.headers.get('accept') == 'text/event-stream':   
        def events():           
            val = ''
            global tasks_to_do
            workflow_name = "TEST"
            myeng = eval(workflow_name)
            current_node = myeng.start_node 
            global user_count
            last_user = ''
            start = 0
            while(current_node != 'NULL'):
                current_node.jobs+=1
                if('User'+str(user_count) in myeng.priority_users):
                    if(start==0):
                        if(current_node.not_executed !=[]):
                            last_user = current_node.not_executed[0]
                        else:
                            last_user = ''
                        current_node.not_executed.insert(0,'User'+str(user_count))
                        current_node.wait_time += current_node.time
                        tasks_to_do.append(current_node)
                        print("^^^^^^^^^^^^",current_node.title)
                        start=1
                    else:
                        if(prev_node.not_executed==[]):
                            ind = 0
                        else:
                            rem = 'NULL'
                            if(prev_node.is_executing):
                                rem=prev_node.executing_user
                            for i in prev_node.not_executed:
                                if(i=='User'+str(user_count)):
                                    break
                                if(i in current_node.not_executed):
                                    rem = i
                            if(rem == 'NULL'):
                                ind = 0
                            else:
                                ind = current_node.not_executed.index(rem)+1
                        current_node.not_executed.insert(ind,'User'+str(user_count))
                        current_node.wait_time += current_node.time
                        tasks_to_do.append(current_node)
                        print("^^^^^^^^^^^^",current_node.title)

                else:
                    current_node.not_executed.append('User'+str(user_count))
                    current_node.wait_time += current_node.time
                    tasks_to_do.append(current_node)
                    print("^^^^^^^^^^^^",current_node.title)
                    print('---------------',current_node.title, current_node.not_executed)
                prev_node = current_node
                if(current_node.output!=[]):
                    temp=float('inf')
                    node_time = float('inf')
                    #print('--------^^^^^^^^^^^^^^________________', current_node.output)
                    for i in current_node.output:
                        if(i.toOperator.wait_time == temp):
                            if(i.toOperator.time < node_time):
                                temp = i.toOperator.wait_time
                                node_time = i.toOperator.time
                                next_link = i
                        if(i.toOperator.wait_time< temp):
                            temp = i.toOperator.wait_time
                            node_time = i.toOperator.time
                            next_link = i
                    current_node = next_link.toOperator
                else:
                    break
            user_count+=1  
            global result 
            # workflow_name = "TEST"
            # myeng = eval(workflow_name)
            val = "Started execution of Workflow Engine"+" "+myeng.name+"\n"
            #print("IS EXECUTING", myeng.is_executing)
            yield "data: %s\n\n" % (val)
            time.sleep(0.1)
            print("Started execution of Workflow Engine"+" "+myeng.name)
            # current_node = myeng.start_node
            task_no = 1
            p = 'p'+str(task_no)
            while(tasks_to_do != []):
                if(tasks_to_do[0].is_executing == False):
                    with concurrent.futures.ThreadPoolExecutor() as executer:
                        future = executer.submit(execute_node, tasks_to_do[0])
                        h = tasks_to_do.pop(0)
                        print(">>>>>>>>>>>>POP", h.title)
                        value = yield from future.result()
                        if(value is not None):
                            yield "data: %s\n\n" % (value)
                else:
                    h = tasks_to_do.pop(0)           
                    print("#############POP", h.title)
                print('TO_DoooooooO',tasks_to_do)
                # task_no+=1
                # p = 'p'+str(task_no)
                # value = yield from execute_node(current_node) 
                # yield "data: %s\n\n" % (value)              
                # if(current_node.output!=[]):
                #     #print(current_node.title, current_node.output.id, current_node.output.toOperator)
                #     #print("IS EXECUTING", myeng.is_executing)
                #     temp=float('inf')
                #     node_time = float('inf')
                #     for i in current_node.output:
                #         if(i.toOperator.time * i.toOperator.jobs == temp):
                #             if(i.toOperator.time < node_time):
                #                 temp = i.toOperator.time * i.toOperator.jobs
                #                 node_time = i.toOperator.time
                #                 next_link = i
                #         if(i.toOperator.time * i.toOperator.jobs < temp):
                #             temp = i.toOperator.time * i.toOperator.jobs
                #             node_time = i.toOperator.time
                #             next_link = i
                #     current_node = next_link.toOperator
                #     #print("_______", current_node.title)
                # else:
                #     break
            #print("IS EXECUTING", myeng.is_executing)
            print("Finished execution of Workflow engine"+" "+myeng.name)
            val = "Finished execution of Workflow Engine"+" "+myeng.name+"\n"
            yield "data: %s\n\n" % (val)
            time.sleep(0.1)  # an artificial delay 
            val = ' '  
            yield "data: %s\n\n" % (val)
            time.sleep(0.1) 
            #yield "data: END-OF-WORKFLOW\n\n"                     
        return Response(events(), content_type='text/event-stream')
    return redirect(url_for('static', filename='demo.html'))
    


@app.route("/calculate_time",methods=["GET"])  
def calculate_time():
    workflow_name = "TEST"
    myeng = eval(workflow_name)
    current_node = myeng.start_node
    myeng.total_time = 0
    while(current_node != 'NULL'):
        myeng.total_time += current_node.time
        if(current_node.output!=[]):
            next_link = current_node.output[0]
            current_node = next_link.toOperator
        else:
            break
    calculated_time = {}
    calculated_time["totalTime"] = myeng.total_time
    resp = jsonify(calculated_time)
    resp.headers['Access-Control-Allow-Origin']='*'
    return resp

if __name__ == "__main__":
    app.secret_key = '123'
    app.debug=True
    app.run()