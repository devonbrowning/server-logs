import json
import os
import random
import time
from datetime import datetime, timezone

DOS_ATTACK_FILE_PATH = "dos.txt"
TARGET_REQUESTS_PER_MINUTE = 100
attacker = "none"
amount_to_attack = -1
attacks_so_far = -1

with open("users.json", "r") as file:
    users = tuple(json.loads(file.read()))


services = ({"route":"/partners","rate":0.01},
            {"route":"/trending","rate":0.001},
            {"route":"/list","rate":0.003},
            {"route":"/search","rate":0.002},
            {"route":"/image/p03OU9ab2","rate":0.004},
            {"route":"/video/e456Y8tv3","rate":0.005},
            {"route":"/image/m397W4iv8","rate":0.004},
            {"route":"/video/r335Q7vg9","rate":0.005})


def generate_timestamp():
  now = datetime.now(timezone.utc)
  return now.isoformat()


def generate_res_code():
    if random.random() <= .1:
      if random.random() < .5:
         return 404
      else:
         return 500
    else:
      return 200
    

def print_api_log(user):
    print(f'{user["ip_address"]} {user["user_name"]} {user["user_id"]} [{generate_timestamp()}] "GET {random.choice(services)["route"]}" { generate_res_code()}')
    

def trigger_dos_attack():
    global attacker
    global attacks_so_far
    global amount_to_attack
    if attacks_so_far < amount_to_attack:
       for _ in range(4):
          print_api_log(attacker)
        #   print("ATTACK!!!!!!!!!!!!!!!!!^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
          time.sleep(random.random()/4)
       

def trigger_api_log():
    user = random.choice(users)
    print_api_log(user)


def is_there_dos_attack():
   # check if file exists
   if os.path.exists(DOS_ATTACK_FILE_PATH):
      return True
   else:
      return False
   


def start_program():
    global attacker
    global attacks_so_far
    global amount_to_attack
    while True:
        delay = round(random.uniform(.2, 1.1),1) 
        if is_there_dos_attack():
            if attacker == "none":
                attacker = random.choice(users)
                amount_to_attack = random.randint(400, 600)
                attacks_so_far = 0

            trigger_dos_attack()
        else:
           attacker = "none"
           amount_to_attack = -1
           attacks_so_far = -1
        
        trigger_api_log()
        time.sleep(delay)     


if __name__ == '__main__':
    start_program()
   

