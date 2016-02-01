__author__ = 'stephencarrow'

import plotly.plotly as py
import plotly.graph_objs as go
from plotly.offline import download_plotlyjs, init_notebook_mode, iplot
init_notebook_mode()
import datetime
import pandas as pd
import igraph
from igraph import *
igraph.__version__
from impala.dbapi import connect
from impala.util import as_pandas
import cufflinks as cf
import math
import numpy as np

def whereClause(eventName, propertyName, operator, propertyValue):
    clause = """(case when event_name = '%s' and %s %s '%s' then true
                      when event_name != '%s' then true
                      else false end) = true""" % (str(eventName), str(propertyName), str(operator), str(propertyValue), str(eventName))
    return clause

def queryEvents(eventLevels,whereClauses,fromRootThresholdPct,fromParentThresholdPct,appName,startEvent,startDate,endDate):
    print "This will take a few minutes"
    vID = 0
    levelID = 1
    if len(whereClauses) > 0:
        where = 'true '
        for eachClause in whereClauses:
            where = where + "and " + eachClause

    #Query Impala
    query = """select distinct
                   0 as vID,
                   0 as levelID,
                   0 as parentVID,
                   event_name,
                   distinct_id
                   from
                   fact_beacon_history
                   where
                   %s
                   and event_name = '%s'
                   and app_name = '%s'
                   and date_sid between '%s' and '%s'
                   """ % (where, startEvent, appName, startDate, endDate)
    conn = connect(host='52.89.99.148', port=21050)
    cur = conn.cursor()
    t0 = datetime.datetime.utcnow()
    cur.execute(query)
    t1 = datetime.datetime.utcnow()
    #print t1-t0
    treeData = as_pandas(cur)
    rootUserCount = len(treeData['distinct_id'])
    vID = 1
    # parentDistinctIDs = pd.DataFrame(pd.unique(treeData['distinct_id'])).values

    while levelID < eventLevels:
        uniques = treeData[treeData['levelid'] == levelID - 1].drop_duplicates(subset=['event_name','vid'],take_last=True)[['event_name','vid']].values
        eventList = treeData[treeData['levelid'] == levelID - 1].drop_duplicates(subset=['event_name','vid'],take_last=True)[['event_name','vid']].values
        for eachEvent, pvid in eventList:
            #Query Impala
            query = """select distinct
                       event_name
                       from
                       fact_beacon_history
                       where
                       %s
                       and app_name = '%s'
                       and prev_event = '%s'
                       and date_sid between '%s' and '%s'""" % (where, appName, eachEvent, startDate, endDate)
            conn = connect(host='52.89.99.148', port=21050)
            cur = conn.cursor()
            t0 = datetime.datetime.utcnow()
            cur.execute(query)
            t1 = datetime.datetime.utcnow()
            #print t1-t0
            events = as_pandas(cur)
            parentVID = pvid
            #parentVID = treeData[(treeData['levelid'] == levelID - 1) & (treeData['event_name'] == eachEvent)]['vid'].values[0]

            for eachSubEvent in events[events['event_name'] != startEvent]['event_name']:
                parentUserCount = len(treeData[(treeData['levelid'] == levelID - 1) & (treeData['event_name'] == eachEvent)])
                parentRootConv = np.true_divide(parentUserCount,rootUserCount)*100
                if parentRootConv > fromRootThresholdPct:
                    query = """select distinct
                               %s as vID,
                               %s as levelID,
                               %s as parentVID,
                               event_name,
                               distinct_id
                               from
                               fact_beacon_history
                               where
                               %s
                               and app_name = '%s'
                               and event_name = '%s'
                               and prev_event = '%s'
                               and date_sid between '%s' and '%s'
                              """ % (vID, levelID, parentVID, where, appName, eachSubEvent, eachEvent, startDate, endDate)
                    conn = connect(host='52.89.99.148', port=21050)
                    cur = conn.cursor()
                    t0 = datetime.datetime.utcnow()
                    cur.execute(query)
                    t1 = datetime.datetime.utcnow()
                    addTreeData = as_pandas(cur)
                    #parentDistinctIDs = treeData[(treeData['levelid'] == levelID - 1) & (treeData['event_name'] == eachEvent)]['distinct_id'].values
                    parentDistinctIDs = treeData[treeData['vid'] == parentVID].values
                    addRecord = False
                    for eachID in addTreeData['distinct_id']:
                        if eachID in parentDistinctIDs:
                            treeData = pd.concat([treeData,addTreeData[addTreeData['distinct_id'] ==  eachID]])
                            addRecord = True
                    tempGraphData = pd.DataFrame({'user_count' : treeData.groupby(['vid','levelid','parentvid','event_name'], as_index = False).size()}).reset_index()
                    nodeUserCount = tempGraphData[tempGraphData['vid'] == max(tempGraphData['vid'])]['user_count']
                    fromParentConv = np.true_divide(nodeUserCount,parentUserCount).values*100
                    if addRecord == True and fromParentConv > fromParentThresholdPct:
                        vID +=1
                    elif addRecord == True:
                        treeData = treeData[treeData['vid'] != max(treeData['vid'])]
        levelID += 1
    #print treeData
    return treeData

def graph(treeData):
    graphData = pd.DataFrame({'user_count' : treeData.groupby(['vid','levelid','parentvid','event_name'], as_index = False).size()}).reset_index()
    graphData['from_root'] = graphData['user_count'].apply(lambda x: round(np.true_divide(x,graphData['user_count'][0]),2))
    fromParent = []
    for index, row in graphData.iterrows():
        parentCount = graphData[graphData['vid'] == row['parentvid']]['user_count']
        fromParent.append(round(np.true_divide(row['user_count'],parentCount),2))
    graphData['from_parent'] = fromParent


    G = Graph()
    G.add_vertices(max(graphData['vid']) + 1)
    for eachEdge in range(1,max(graphData['vid']) + 1):
        G.add_edges([(int(graphData['parentvid'].iloc[eachEdge]),graphData['vid'].iloc[eachEdge])])


    nr_vertices = max(graphData['vid']) + 1
    v_label = []
    for row in graphData[['event_name','from_parent','from_root']].values:
        v_label.append(str(row[0] + "<br>From Parent: " + str(round(row[1]*100.0,1)) + "%</br>" + "From Root: " + str(round(row[2]*100.0,1)) + "%"))
    # # print "v_label: ", v_label
    lay = G.layout_reingold_tilford(mode="out", root=[0])
    #lay.fit_into(bbox=(600,1000), keep_aspect_ratio=False)


    position = {k: lay[k] for k in range(nr_vertices)}
    # # print "position: ", position
    Y = [lay[k][1] for k in range(nr_vertices)]
    # # print "Y values: ", Y
    M = max(Y)
    vFactor = 5

    es = EdgeSeq(G) # sequence of edges
    # # print "edge sequence: ", es, G.es
    E = [e.tuple for e in G.es] # list of edges
    # # print "list of edges: ", E

    L = len(position)
    Xn = [position[k][0] for k in range(L)]
    Yn = [vFactor*M-position[k][1] for k in range(L)]
    Xe = []
    Ye = []
    Ls = []
    Le = []
    Re = []
    for edge in E:
        Xe+=[position[edge[0]][0],position[edge[1]][0], None]
        Ye+=[vFactor*M-position[edge[0]][1],vFactor*M-position[edge[1]][1], None]
        Ls+=[[position[edge[0]][0],vFactor*M-position[edge[0]][1]]]
        # print math.pow(graphData[graphData['vid']==edge[1]]['user_count'].values[0],2)/1000000
        if position[edge[1]][0] < 0:
            Le+=[[position[edge[1]][0]+math.pow((1+graphData[graphData['vid']==edge[1]]['user_count'].values)[0],1.5)/200000,vFactor*M-position[edge[1]][1]]]
            Re+=[[position[edge[1]][0]-math.pow((1+graphData[graphData['vid']==edge[1]]['user_count'].values)[0],1.5)/200000,vFactor*M-position[edge[1]][1]]]
        else:
            Le+=[[position[edge[1]][0]-math.pow((1+graphData[graphData['vid']==edge[1]]['user_count'].values)[0],1.5)/200000,vFactor*M-position[edge[1]][1]]]
            Re+=[[position[edge[1]][0]+math.pow((1+graphData[graphData['vid']==edge[1]]['user_count'].values)[0],1.5)/200000,vFactor*M-position[edge[1]][1]]]
    # # print Xe
    # # print Ye
    # # print "Le ", Le
    # # print "Re ", Re
    # # print Ls

    labels = v_label

    lines = []
    for i in range(len(Le)):
        lines.append(dict(
                      fillcolor='rgb(47,79,79)',
                      path="""M%s,%s C%s,%s %s,%s %s,%s M%s,%s C%s,%s %s,%s %s,%s H%s""" % (str(Ls[i][0]),str(Ls[i][1]),
                                                                               str(Le[i][0]),str(Le[i][1]+0.5),
                                                                               str(Le[i][0]),str(Le[i][1]),
                                                                               str(Le[i][0]),str(Le[i][1]),
                                                                               str(Ls[i][0]),str(Ls[i][1]),
                                                                               str(Re[i][0]),str(Re[i][1]+0.5),
                                                                               str(Re[i][0]),str(Re[i][1]),
                                                                               str(Re[i][0]),str(Re[i][1]),
                                                                               str(Le[i][0])
                                                                              ),
                      xref='x',
                      yref='y',
                      line=dict(
                        color='rgb(47,79,79)',
                        width=1
                      ),
                     opacity=0.75
                    )
        )

    dots = go.Scatter(x=Xn,
              y=Yn,
              mode='markers',
              name='',
              marker=dict(symbol='dot',
                            size=10,
                            color='rgb(125,158,192)',    #'#DB4551',
                            line=dict(color='rgb(50,50,50)', width=1)
                            ),
              text=labels,
              hoverinfo='text',
              opacity=0.8
              )

    axis = dict(showline=False, # hide axis line, grid, ticklabels and  title
        zeroline=False,
        showgrid=False,
        showticklabels=False,
        )

    layout = dict(title= 'User Flows Tree',
                  #annotations=make_annotations(position, v_label),
                  font=dict(size=12),
                  showlegend=False,
                  xaxis=go.XAxis(axis),
                  yaxis=go.YAxis(axis),
                  margin=dict(l=40, r=40, b=85, t=100),
                  hovermode='closest',
                  plot_bgcolor='rgb(248,248,248)' ,
                  shapes=lines,
                  height=800
            )

    data=go.Data([dots])
    fig=dict(data=data, layout=layout)
    #fig['layout'].update(annotations=make_annotations(position, v_label))
    iplot({'data': data, 'layout': layout})

