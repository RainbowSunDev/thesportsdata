import os
import pandas as pd
import requests
import time
import datetime
import asyncio


async def get_data():
        
    file_path = 'output.csv' 

    # Prepare a list to store output data
    user="ecentrix"
    secret="c9db4c7ba3b10baa47207889e206f95b"
    detail_live_url = f'https://api.thesports.com/v1/football/match/detail_live?user={user}&secret={secret}'  
    match_list_url = "https://api.thesports.com/v1/football/match/recent/list?user=ecentrix&secret=c9db4c7ba3b10baa47207889e206f95b&uuid="
    team_list_url = "https://api.thesports.com/v1/football/team/additional/list?user=ecentrix&secret=c9db4c7ba3b10baa47207889e206f95b&uuid="

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
    }
   

    async def convert_to_localtime(timestamp):
        print("timestamp:", timestamp)
        # Convert to local date-time
        local_time = datetime.datetime.fromtimestamp(timestamp)
        return local_time
    async def get_current_time():
        current_time = datetime.datetime.now().time()
        formatted_time = current_time.strftime('%H:%M:%S')
        return formatted_time
    async def get_matchstatus_string(matchstatus):
        """Converts matchstatus to a string representation."""
        statuses = {
            0: "Abnormal", 1: "Not started", 2: "First half", 3: "Half-time", 
            4: "Second half", 5: "Overtime", 6: "Overtime", 7: "Penalty Shoot-out", 
            8: "End", 9: "Delay", 10: "Interrupt", 11: "Cut in half", 
            12: "Cancel", 13: "To be determined"
        }
        return statuses.get(matchstatus, "Unknown")
    async def check_match_id_exist(match_id):
        # Check if the CSV file exists
        if os.path.exists(file_path):
            # Read the CSV file into a DataFrame
            df = pd.read_csv(file_path)

            # Filter the DataFrame based on the live_match_id
            filtered_df = df[df['live_match_id'] == match_id]
            
            # Check if the filtered DataFrame is empty
            if not filtered_df.empty:
                home_name = filtered_df['home_name'].iloc[0]
                away_name = filtered_df['away_name'].iloc[0]
                return [True, home_name, away_name]
            else:
                return [False]
        else:
            return [False]
    async def get_team_name(team_id):
        team_name_response = requests.get(team_list_url+team_id, headers=headers)
        time.sleep(0.1)
        team_data = team_name_response.json()
        team_name = team_data['results'][0]['name']
        return team_name
    
    async def get_output_data():
        output_data = []

         # Make a GET request to the URL
        response = requests.get(detail_live_url, headers=headers)
        data = response.json()
        results = data['results']
        for result in results:
            match_id = result['id']
            is_exist_match_id = await check_match_id_exist(match_id)
            if is_exist_match_id[0]:
                print("exist")
                home_team_name = is_exist_match_id[1]
                away_team_name = is_exist_match_id[2]
                updated_at=await get_current_time()
            else:
                match_list_response = requests.get(match_list_url+match_id, headers=headers)
                await asyncio.sleep(0.01)
                print("matchid",match_id)
                match_list = match_list_response.json()
                if len(match_list['results']) == 0:
                    continue
                home_team_id=match_list['results'][0]['home_team_id']
                away_team_id=match_list['results'][0]['away_team_id']
                updated_at = await convert_to_localtime(match_list['results'][0]['updated_at'])
                home_team_name = await get_team_name(home_team_id)
                away_team_name = await get_team_name(away_team_id)

            score = result['score']
            stats = result['stats']

            match_status = score[1]
            home_data = score[2]
            away_data = score[3]
            kickoff_time = score[4]
            
            home_score = home_data[0]
            away_score = away_data[0]
            for stat in stats:
                if stat['type'] == 1:
                    home_goal = stat['home'] 
                    away_goal = stat['away']
                if stat['type'] == 2:
                    home_corners = stat['home'] 
                    away_corners = stat['away']
                if stat['type'] == 3:
                    home_yellow_card = stat['home'] 
                    away_yellow_card = stat['away']
                if stat['type'] == 4:
                    home_red_card = stat['home'] 
                    away_red_card = stat['away']
                if stat['type'] == 5:
                    home_offside = stat['home'] 
                    away_offside = stat['away']
                if stat['type'] == 6:
                    home_free_kick = stat['home'] 
                    away_free_kick = stat['away']
                if stat['type'] == 7:
                    home_goal_kick = stat['home'] 
                    away_goal_kick = stat['away']
                if stat['type'] == 8:
                    home_penalty = stat['home'] 
                    away_penalty = stat['away']
                if stat['type'] == 21:
                    home_shots_on_target = stat['home'] 
                    away_shots_on_target = stat['away'] 
                if stat['type'] == 22:
                    home_shots_off_target = stat['home'] 
                    away_shots_off_target = stat['away'] 
                if stat['type'] == 23:
                    home_attacks = stat['home'] 
                    away_attacks = stat['away'] 
                if stat['type'] == 24:
                    home_dangerous_attacks = stat['home'] 
                    away_dangerous_attacks = stat['away']
                if stat['type'] == 25:
                    home_possession = stat['home'] 
                    away_possession = stat['away']
                
            # Prepare the output row - token address and whether the image exists
            output_row = {
            'live_match_id': match_id, 
            'kickoff_time': "" if kickoff_time == 0 else await convert_to_localtime(kickoff_time), 
            'match_status': await get_matchstatus_string(match_status), 
            'home_name': "" if "home_team_name" not in locals() else home_team_name, 
            'home_score': "" if "home_score" not in locals() else home_score, 
            'home_yellow_card': "" if "home_yellow_card" not in locals() else home_yellow_card, 
            'home_red_card': "" if "home_red_card" not in locals() else home_red_card, 
            'home_offside': "" if "home_offside" not in locals() else home_offside, 
            'home_free_kick': "" if "home_free_kick" not in locals() else home_free_kick, 
            'home_goal_kick': "" if "home_goal_kick" not in locals() else home_goal_kick, 
            'home_penalty': "" if "home_penalty" not in locals() else home_penalty, 
            'home_shots_on_target': "" if "home_shots_on_target" not in locals() else home_shots_on_target, 
            'home_shots_off_target': "" if "home_shots_off_target" not in locals() else home_shots_off_target, 
            'home_attacks': "" if "home_attacks" not in locals() else home_attacks, 
            'home_dangerous_attacks': "" if "home_dangerous_attacks" not in locals() else home_dangerous_attacks, 
            'home_corners': "" if "home_corners" not in locals() else home_corners, 
            'home_possession': "" if "home_possession" not in locals() else home_possession, 
            'away_name': "" if "away_team_name" not in locals() else away_team_name, 
            'away_score': "" if "away_score" not in locals() else away_score,
            'away_yellow_card': "" if "away_yellow_card" not in locals() else away_yellow_card, 
            'away_red_card': "" if "away_red_card" not in locals() else away_red_card, 
            'away_offside': "" if "away_offside" not in locals() else away_offside, 
            'away_free_kick': "" if "away_free_kick" not in locals() else away_free_kick, 
            'away_goal_kick': "" if "away_goal_kick" not in locals() else away_goal_kick, 
            'away_penalty': "" if "away_penalty" not in locals() else away_penalty, 
            'away_shots_on_target': "" if "away_shots_on_target" not in locals() else away_shots_on_target, 
            'away_shots_off_target': "" if "away_shots_off_target" not in locals() else away_shots_off_target, 
            'away_attacks': "" if "away_attacks" not in locals() else away_attacks, 
            'away_dangerous_attacks': "" if "away_dangerous_attacks" not in locals() else away_dangerous_attacks, 
            'away_corners': "" if "away_corners" not in locals() else away_corners, 
            'away_possession': "" if "away_possession" not in locals() else away_possession, 
            'created_at': await get_current_time(), 
            'updated_at': updated_at, 
            # 'match_info': match_info, 
            # 'match_time': match_time, 
            # 'match_minute': match_minute, 
            # 'match_second': match_second, 
            # 'commentary': commentary, 
            # 'extra_time': extra_time, 
            # 'is_active': is_active, 
            # 'is_suspended': is_suspended, 
            }
            # Append the output row to the output data list
            output_data.append(output_row)
        return output_data
    output_data = await get_output_data()
    # Convert the output data list to a pandas DataFrame
    output_df = pd.DataFrame(output_data)
        
    # Check if the file already exists
    file_exists = False
    try:
        with open('output.csv', 'r') as f:
            file_exists = True
    except FileNotFoundError:
        pass

    # If file exists, append without writing header, otherwise create new file with header
    if file_exists:
        output_df.to_csv('output.csv', mode='a', header=False, index=False)
    else:
        output_df.to_csv('output.csv', index=False)