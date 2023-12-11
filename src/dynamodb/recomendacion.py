import boto3
import pandas as pd
import numpy as np
from scipy.spatial.distance import cityblock
from flask import Flask, jsonify
import json

app = Flask(__name__)

aws_access_key_id = ''
aws_secret_access_key = ''
region_name = 'us-east-1'
table_name = 'Dynamo-LV-1'

dynamodb = boto3.resource('dynamodb', 
                          aws_access_key_id = aws_access_key_id,
                          aws_secret_access_key = aws_secret_access_key,
                          region_name = region_name)

dynamo_table = dynamodb.Table('Dynamo-LV-1')

def database_to_pandas(table, num_rows=10):
    response = table.scan(Limit=num_rows)
    items = response['Items']
    df = pd.DataFrame(items)
    return df

def computeNearestNeighbor(dataframe, target_user, distance_metric=cityblock):
    distances = np.zeros(len(dataframe))  
    for i, (_, row) in enumerate(dataframe.iterrows()):
        distance = distance_metric(dataframe.loc[target_user].fillna(0), row.fillna(0))
        distances[i] = distance
    sorted_indices = np.argsort(distances)
    sorted_distances = distances[sorted_indices]
    return list(zip(dataframe.index[sorted_indices], sorted_distances))

def consolidate_data(df):
    consolidated_df = df.groupby(['user_id', 'course_id'])['stars_eng'].mean().unstack()
    return consolidated_df

def recommend_courses_from_dynamodb(user_id, df):
    nearest = computeNearestNeighbor(consolidated_df, user_id)
    courses_completed_by_user = set(df[df['user_id'] == user_id]['course_id'])

    recommendations = {}
    for neighbor, distance in nearest[1:6]:  
        neighbor_courses = set(df[df['user_id'] == neighbor]['course_id'])

        for course in neighbor_courses:
            if course not in courses_completed_by_user:
                if course not in recommendations:
                    recommendations[course] = 1
                else:
                    recommendations[course] += 1

    sorted_recommendations = sorted(recommendations.items(), key=lambda x: x[1], reverse=True)
    return sorted_recommendations

target_user_id = 54

df = database_to_pandas(dynamo_table, num_rows=10)
consolidated_df = consolidate_data(df)

@app.route('/<int:user_id>', methods=['GET'])
def get_recommendations(user_id):
    try:

        sorted_recommendations = recommend_courses_from_dynamodb(user_id, df)
        recommendations_list = [{"course_id": int(course), "rating": count} for course, count in sorted_recommendations]

        recommendations_json = json.dumps({"recomendaciones": recommendations_list}, indent=4)

        return jsonify(json.loads(recommendations_json))
    
    except Exception as ex:
        return jsonify({"error": str(ex)})
    
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=80, debug=True)
