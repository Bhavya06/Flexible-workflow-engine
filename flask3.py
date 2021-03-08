from flask import Flask, make_response, request, render_template, jsonify, Response, redirect,url_for, stream_with_context
import time
import json
from json import JSONEncoder
from time import sleep
from multiprocessing import Process, Manager
import threading
import concurrent.futures

from flask_cors import CORS, cross_origin

app = Flask(__name__,static_url_path="/",static_folder='C:/Users/USER/Desktop/proj', template_folder='templates')

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
    def __init__(self, id, title, time, input, output, not_executed, fin_executed, jobs, is_executing, executing_user):
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

global result
result = {}
result['operators'] = {}
result['links'] = {}
global operator_data
global operatorI
operatorI = 0
operator_data={}
global task_user
task_user = {}


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
    operatorId = 'created_operator_' + str(operatorI)
    operatorData = {
        "top": (400 / 2) - 30,
        "properties": {
            "title": 'Task ' + str(operatorI + 1),
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
    task_name = operatorId
    time = 0
    globals()[task_name] = Operator(task_name,'Task ' + str(operatorI + 1) + '[ Time: 0 ]', time,'NULL', 'NULL',[],[],0, False, 'NULL')      
    if(myeng.start_node == 'NULL'):
        myeng.start_node = eval(task_name) 
        myeng.last_node = eval(task_name)
    print("FIRST NODE LAST NODE", myeng.start_node.title, myeng.last_node.title)
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
    global task_user
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
        task_user[eval(linkDataOriginal["toOperator"]).title] = not_executed
        print(task_user)
    eval(linkDataOriginal["toOperator"]).not_executed = not_executed
    #print(eval(linkDataOriginal["toOperator"]).not_executed, eval(linkDataOriginal['toOperator']).jobs )
    globals()[link_name] = Link(link_name,eval(linkDataOriginal['fromOperator']), linkDataOriginal['fromConnector'],eval(linkDataOriginal['toOperator']), linkDataOriginal['toConnector'])
    #print("From operator", linkDataOriginal['fromOperator'], "To operator", linkDataOriginal["toOperator"])
    task_name  = linkDataOriginal['fromOperator']
    globals()[task_name].output = eval(link_name)
    task_name  = linkDataOriginal['toOperator']
    globals()[task_name].input = eval(link_name)
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
        if(eval(operatorId).output!='NULL'):
            myeng.start_node = eval(operatorId).output.toOperator
        else:
            myeng.start_node = 'NULL'

    if(eval(operatorId).input!='NULL'):
        eval(operatorId).input.toOperator='NULL'
        eval(operatorId).input.toConnector = 'NULL'
    if(eval(operatorId).output!='NULL'):
        eval(operatorId).output.fromOperator='NULL'
        eval(operatorId).output.fromConnector = 'NULL'    
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
        globals()[linkId].toOperator.input='NULL'
    if(globals()[linkId].fromOperator!='NULL'):
        globals()[linkId].fromOperator.output='NULL'
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

def execute_node(current_node, user_name):
    print(current_node.title, user_name)
    workflow_name = "TEST"
    myeng = eval(workflow_name)
    if(current_node.is_executing == False):
        prev_link = current_node.input
        # if(prev_link=='NULL'):
        #     print("CURRENT   LINK",current_node.title,prev_link)
        # else:
        #     print("CURRENT   LINK",current_node.title,prev_link.id,current_node.not_executed)
        if(prev_link == 'NULL'):
            #print("IN IF")
            while(current_node.jobs>0 and current_node.not_executed != []):
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
                if(current_node.not_executed!=[]):
                    user_name = current_node.not_executed[0]
                current_node.is_executing = False
        else:
            print("IN ELSE")
            prev_node = prev_link.fromOperator
            c=0
            while(current_node.not_executed == [] or current_node.is_executing == True or current_node.not_executed[0]!=user_name or current_node.not_executed[0] not in prev_node.fin_executed):
                c+=1
            while(True):
                while(current_node.jobs>0 and current_node.not_executed != [] and current_node.is_executing == False and current_node.not_executed[0] in prev_node.fin_executed and current_node.not_executed[0]==user_name):
                    print("IN WHILE")
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
                    current_node.fin_executed.append(not_exec)
                    current_node.executing_user = 'NULL'
                    current_node.jobs-=1 
                    if(current_node.not_executed!=[]):
                        user_name = current_node.not_executed[0]
                    current_node.is_executing = False
                    
        current_node.is_executing = False
        if(myeng.last_node.is_executing==True or myeng.last_node.not_executed!=[]):
            myeng.is_executing=True
        else:
            myeng.is_executing=False
    

@app.route("/execute_engine",methods=["GET"])  
def execute_engine(): 
    global task_user
    if request.headers.get('accept') == 'text/event-stream':   
        def events():           
            val = ''
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
                        if(current_node.title not in task_user):
                            task_user[current_node.title] = ['User'+str(user_count)]
                        else:
                            task_user[current_node.title].insert(0, 'User'+str(user_count))
                        start=1
                    else:
                        if(last_user != ''):
                            ind = current_node.not_executed.index(last_user)
                        else:
                            ind = 0
                        current_node.not_executed.insert(ind,'User'+str(user_count))
                        if(current_node.title not in task_user):
                            task_user[current_node.title] = ['User'+str(user_count)]
                        else:
                            task_user[current_node.title].insert(ind, 'User'+str(user_count))

                else:
                    current_node.not_executed.append('User'+str(user_count))
                    if(current_node.title not in task_user):
                        task_user[current_node.title] = ['User'+str(user_count)]
                    else:
                        task_user[current_node.title].append('User'+str(user_count))
                if(current_node.output!='NULL'):
                    next_link = current_node.output
                    current_node = next_link.toOperator
                else:
                    break
            print(task_user)
            user_count+=1  
            global result 
            workflow_name = "TEST"
            myeng = eval(workflow_name)
            val = "Started execution of Workflow Engine"+" "+myeng.name+"\n"
            #print("IS EXECUTING", myeng.is_executing)
            yield "data: %s\n\n" % (val)
            time.sleep(2)
            print("Started execution of Workflow Engine"+" "+myeng.name)
            current_node = myeng.start_node
            task_no = 1
            p = 'p'+str(task_no)
            while(current_node != 'NULL'):
                # globals()[p] = Process(target=execute_node, args=(current_node,))
                # globals()[p] = threading.Thread(target=execute_node, args=(current_node,))
                # eval(p).start()
                # eval(p).join()
                with concurrent.futures.ThreadPoolExecutor() as executer:
                    print(task_user)
                    future = executer.submit(execute_node, current_node,task_user[current_node.title][0] )
                    task_user[current_node.title].pop(0)
                    print(task_user)
                    #print("^^^^^^^^^")
                    value = yield from future.result()
                    if(value is not None):
                        yield "data: %s\n\n" % (value)            
                task_no+=1
                p = 'p'+str(task_no)
                #value = yield from execute_node(current_node) 
                #yield "data: %s\n\n" % (value)     
                print(current_node.output)         
                if(current_node.output!='NULL'):
                    #print(current_node.title, current_node.output.id, current_node.output.toOperator)
                    #print("IS EXECUTING", myeng.is_executing)
                    next_link = current_node.output
                    current_node = next_link.toOperator
                    #print("_______", current_node.title)
                else:
                    break
            #print("IS EXECUTING", myeng.is_executing)
            print("Finished execution of Workflow engine"+" "+myeng.name)
            val = "Finished execution of Workflow Engine"+" "+myeng.name+"\n"
            yield "data: %s\n\n" % (val)
            time.sleep(2)  # an artificial delay 
            val = ' '  
            yield "data: %s\n\n" % (val)
            time.sleep(2) 
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
        if(current_node.output!='NULL'):
            next_link = current_node.output
            current_node = next_link.toOperator
        else:
            break
    calculated_time = {}
    calculated_time["totalTime"] = myeng.total_time
    resp = jsonify(calculated_time)
    resp.headers['Access-Control-Allow-Origin']='*'
    return resp

'''
@app.route("/api/v1/workflow/current_task/<workflow_name>",methods=["GET"])  
def current_task(workflow_name):
    myeng = eval(workflow_name)
    return make_response(myeng.current_node,200)
'''
if __name__ == "__main__":
    app.secret_key = '123'
    app.debug=True
    app.run()