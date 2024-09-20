from faker import Faker
import random
import json
from random_ip_generator import random_ip_for_country

fake = Faker()

num_of_users = 300

# make ip addresses
# ----------------------------------------------------------------
random_ips = []
country_code = "US"

for _ in range(num_of_users):
   random_ips.append(str(random_ip_for_country(country_code)))

# make user ids
# ----------------------------------------------------------------
def generate_unique_numbers(num_digits, num_numbers):

  # Create an empty set to store unique numbers
  unique_numbers = set()

  # Loop until we have the desired number of unique numbers
  while len(unique_numbers) < num_numbers:
    # Generate a random 4-digit integer (as a string)
    number = str(random.randint(10**(num_digits-1), 10**num_digits - 1))

    # Add the number to the set if it's unique
    unique_numbers.add(number)

  return unique_numbers


unique_ids = list(generate_unique_numbers(4, num_of_users))

# make users
# ----------------------------------------------------------------
random_users = []

for num in range (num_of_users):
    random_users.append({"user_name": fake.name().lower().replace(" ", "_"), "user_id": unique_ids[num], "ip_address": random_ips[num] })


# write data
# ----------------------------------------------------------------
# print(random_users)
# Convert to JSON string 
json_users = json.dumps(random_users)

# Write to txt file
with open("users.json", "w") as file:
    file.write(json_users)


