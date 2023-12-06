import psycopg2

import numpy as np
import pandas as pd
from scipy.spatial.distance import cityblock

from sqlalchemy import create_engine

import json

from flask import Flask, jsonify

PG_HOST = 'testing-db-con.cqqmjhk2qryw.us-east-1.rds.amazonaws.com'
PG_DATABASE = 'gestionCursos'
PG_USER = 'root'
PG_PASSWORD = 'postgres'

def database_to_pandas(engine, num_rows=10):
    query = f"SELECT user_id, course_id, stars_eng FROM {nombre_tabla} LIMIT {num_rows};"
    df = pd.read_sql_query(query, engine)
    return df

def computeNearestNeighbor(dataframe, target_user, distance_metric=cityblock):
    distances = np.zeros(len(dataframe))  # Initialize a NumPy array
    # Iterate over each row (user) in the DataFrame
    for i, (index, row) in enumerate(dataframe.iterrows()):
        if index == target_user:
            continue  # Skip the target user itself
        # Calculate the distance between the target user and the current user
        distance = distance_metric(dataframe.loc[target_user].fillna(0), row.fillna(0))
        distances[i] = distance
    # Get the indices that would sort the array, and then sort the distances accordingly
    sorted_indices = np.argsort(distances)
    sorted_distances = distances[sorted_indices]
    return list(zip(dataframe.index[sorted_indices], sorted_distances))

def consolidate_data(df):
    # Group by 'userId' and 'movieId' and calculate the mean of 'rating'
    consolidated_df = df.groupby(['user_id', 'course_id'])['stars_eng'].mean().unstack()
    return consolidated_df

def recommend_courses_from_db(user_id, df):
    nearest = computeNearestNeighbor(consolidated_df, user_id)
    courses_completed_by_user = set(df[df['user_id'] == user_id]['course_id'])

    recommendations = {}
    for neighbor, distance in nearest[1:6]:  # Exclude the user itself
        neighbor_courses = set(df[df['user_id'] == neighbor]['course_id'])

        for course in neighbor_courses:
            if course not in courses_completed_by_user:
                if course not in recommendations:
                    recommendations[course] = 1
                else:
                    recommendations[course] += 1

    sorted_recommendations = sorted(recommendations.items(), key=lambda x: x[1], reverse=True)
    
    return sorted_recommendations

    #print(f"Recommendations for user {user_id} are:")
    #for course, count in sorted_recommendations[:5]:
    #    print(f"Course ID: {course}, Count: {count}")

try:
    engine = create_engine(f'postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}/{PG_DATABASE}')
    print('conexion exitosa')
    nombre_tabla = 'users.tblengagement_eng'

    df = database_to_pandas(engine, num_rows=10)
    
    consolidated_df = consolidate_data(df)
    
    target_user_id = 44
    
except Exception as ex:
    print(ex)
finally:
    if 'engine' in locals():
        engine.dispose()  # Close SQLAlchemy connection

app = Flask(__name__)

@app.route('/<int:user_id>', methods=['GET'])
def get_recommendations(user_id):
    try:
        engine = create_engine(f'postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}/{PG_DATABASE}')
        nombre_tabla = 'users.tblengagement_eng'

        df = database_to_pandas(engine, num_rows=10)
        consolidated_df = consolidate_data(df)

        sorted_recommendations = recommend_courses_from_db(user_id, df)
        recommendations_list = [{"course_id": course, "rating": count} for course, count in sorted_recommendations]

        recommendations_json = json.dumps({"recomendaciones": recommendations_list}, indent=4)

        return jsonify(json.loads(recommendations_json))
    
    except Exception as ex:
        return jsonify({"error": str(ex)})
    
    finally:
        if 'engine' in locals():
            engine.dispose()  # Close SQLAlchemy connection

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=80, debug=True)
