import datetime
from faker import Faker
import random
import csv

fake = Faker()

# Set the number of rows to generate
num_rows = 1000000

# Define the column headers
headers = ['date', 'name', 'age', 'city', 'state', 'fare', 'gender', 'occupation']

CITIES = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose", "Austin", "Jacksonville", "Fort Worth", "Columbus", "San Francisco", "Charlotte", "Indianapolis", "Seattle", "Denver", "Washington", "Boston", "Nashville", "El Paso", "Detroit", "Memphis", "Portland", "Oklahoma City", "Las Vegas", "Louisville", "Baltimore", "Milwaukee", "Albuquerque", "Tucson", "Fresno", "Mesa", "Sacramento", "Atlanta", "Kansas City", "Colorado Springs", "Miami", "Raleigh", "Omaha", "Long Beach", "Virginia Beach", "Oakland", "Minneapolis", "Tulsa", "Wichita", "New Orleans", "Arlington"]

STATES = ["Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut", "Delaware", "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire", "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming"]

OCCUPATIONS = ["Software Engineer", "Data Scientist", "Teacher", "Nurse", "Doctor", "Lawyer", "Accountant", "Sales Manager", "Marketing Specialist", "Writer", "Chef", "Electrician", "Plumber", "Mechanic", "Architect", "Artist", "Musician", "Pilot", "Police Officer", "Firefighter"]

# Open a new CSV file for writing
with open('input.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)

    # Write the header row
    writer.writerow(headers)

    # Generate fake data and write to the CSV file
    for i in range(num_rows):
        # Generate a fake date between 2020 and 2023
        date = fake.date_between_dates(date_start=datetime.date(2020, 1, 1), date_end=datetime.date(2023, 12, 31))

        # Generate a fake name
        name = fake.name()

        # Generate a random age between 18 and 80
        age = random.randint(18, 80)

        # Generate a fake city and state
        city = random.choice(CITIES)
        state = random.choice(STATES)

        # Generate a random fare between 10 and 100
        fare = round(random.uniform(10, 100), 2)

        # Generate a random gender
        gender = random.choice(['Male', 'Female'])

        # Generate a random occupation
        occupation = random.choice(OCCUPATIONS)

        # Write the data to the CSV file
        writer.writerow([date, name, age, city, state, fare, gender, occupation])