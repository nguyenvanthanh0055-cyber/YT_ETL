import logging

logger = logging.getLogger(__name__)
table = 'yt_api'

def insert_data(cur, conn, schema, row):
    try:
        if schema == 'staging':
            video_id = 'video_id'

            cur.execute(f"""
                        INSERT INTO {schema}.{table} ("Video_ID", "Video_Title", "Upload_Date", "Duration", "Video_Views", "Like_Count", "Comments_Count")
                        VALUES(%(video_id)s, %(title)s, %(publishedAt)s, %(duration)s, %(viewCount)s, %(likeCount)s, %(commentCount)s)
                        """, row
                        )
            
        else:
            video_id = "Video_ID"
            
            cur.execute(f"""
                        INSERT INTO {schema}.{table} ("Video_ID", "Video_Title", "Upload_Date", "Duration", "Video_Type" ,"Video_Views", "Like_Count", "Comments_Count")
                        VALUES(%(video_id)s, %(title)s, %(publishedAt)s, %(duration)s, %(Video_Type)s,%(viewCount)s, %(likeCount)s, %(commentCount)s)
                        """, row
                        )
        
        conn.commit()
        logger.info(f"Inserted row with Video_ID: {row[video_id]} ")
    except Exception as e:
        logger.error(f"Error inserting row with Video_ID: {row[video_id]}")
        raise e
            
def update_data(cur, conn, schema, row):
    
    try: 
        if schema == 'staging':
            video_id = "video_id"
            upload_date = "publishedAt"
            video_title = 'title'
            video_views = 'viewCount'
            likes_count = 'likeCount'
            comments_count = "commentCount"
        else:
            video_id = "Video_ID"
            upload_date = "Upload_Date"
            video_title = 'Video_Title'
            video_views = 'Video_Views'
            likes_count = 'Likes_Count'
            comments_count = "Comments_Count"
            
        cur.execute(
            f"""
            UPDATE {schema}.{table} 
            SET "Video_Title = %({video_title})s,
                "Video_Views = %({video_views})s,
                "Likes_Count = %({likes_count})s,
                "Comments_Count = %({comments_count})s
            WHERE "Video_ID" = %({video_id})s AND "Upload_Date" = %({upload_date})s
            """, row
        )

        conn.commit()
        
        logger.info(f"Updated row with Video_ID: {row[video_id]}")
        
    except Exception as e:
        logger.error(f"Error updating row with Video_ID: {row[video_id]} - {e}")
        raise e
    
def delete_data(cur, conn, schema, ids_to_delete):
    try:
        # ids_to_delete = f"""({', '.join(f"'{id}'" for id in ids_to_delete)}) """
        if ids_to_delete:
            sql = f"""
            DELETE FROM {schema}.{table}
            WHERE "Video_ID" = ANY(%s)
            """
            cur.execute(sql, (ids_to_delete,))

            conn.commit()
            logger.info(f"Deleted row with Video_IDs: {ids_to_delete}")
        else:
            logger.warn("There has no Video_ID to delete")

    except Exception as e:
        logger.error(f"Error deleting rows with Video_IDs: {ids_to_delete} - {e}")
        raise e
    
