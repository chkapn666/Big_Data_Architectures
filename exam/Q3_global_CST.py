from mrjob.job import MRJob
from mrjob.step import MRStep
import heapq
import datetime

class CyberThreadAnalyzer(MRJob):
    linec = 0  # i use this class variable to avoid taking into account the first line of the file, which contains the headers
    def mapper(self, _, line):
        CyberThreadAnalyzer.linec += 1 

        # I do not need to ingest the first line (aka the header line)
        if CyberThreadAnalyzer.linec == 1:
            return

        # Each mapper is assigned a particular portion of a file, which is expressed as a particular group of lines. 
        # Each mapper will parse their delegated portion line by line. 
        # I need to make sure that every line parsed comes in the proper form (aka has all the required fields and in the correct format), or I ignore it.
        row = line.strip().split(',')
        try:
            country = row[0]
            year = int(row[1])
            attack_type = row[2]
            target_industry = row[3]
            fin_loss = float(row[4])
            no_affected_users = int(row[5])
            vulnerability_type = row[7]
            resolution_time = int(row[9])

            # self.all_records.append((country, year, laattack_typetitude, target_industry, fin_loss, no_affected_users, vulnerability_type, resolution_time))

        except:
            return  # Ignore invalid lines


        # For Q1 -> I want each mapper to emit (year, fin_loss_incident) pairs, that will later be combined as (year, [fin_loss_incident1, fin_loss_incident2,...]) to
        # be fed into the reducer so that the latter just sums the underlying values. 
        yield f"q1_{year}", fin_loss

        # For Q2 -> Each mapper emits (attack_type, 1) pairs, so that the reducer sees (attack_type1, [1,1,1,1,1,...]), (attack_type2,[1,1,1,1,...])
        # pairs and it just sums the '1' values that represent the occurrence of an attack of the relevant type.
        yield f"q2_{attack_type}", 1

        # For Q3 -> each mapper emits (industry, no_affected_users) pairs, so that the reducer sees (industry1, [no1, no2, ...]), (industry2, [no1,no2,...])
        # pairs and just sum the values in the list to get the totals per industry
        yield f"q3_{target_industry}", no_affected_users

        # For Q4 -> the mappers will only emit pairs when they come across a record/row that contains data on a 'Zero Day'-type attack
        # Each row only refers to a single attack, so I need to only account for its occurrence per country.
        if vulnerability_type == "Zero-day":
            yield f"q4_{country}", 1

        # For Q5 -> the mappers will feed all resolution times for every country into the reducer, and the reducer will just calculate the min
        # Implementing a min-heap approach to get only the min values that every mapper detects for each country would be very 
        # complicated, as I would have to maintain multiple heaps.
        yield f"q5_{country}", resolution_time


    def reducer(self, key, values):
        if key.startswith("q1_"):
            fin_loss_values_for_considered_year = list(values)
            # They key is structured like "q1_2014", "q1_2015", ..., so the year value will start from the 4th (aka index=3) character of the key string
            year_considered = key[3:]
            # We need the total financial loss for each year, so I sum the relevant values
            yield f"q1_total_fin_loss_for_{year_considered}", sum(fin_loss_values_for_considered_year)


        if key.startswith("q2_"):
            attack_occurrences = list(values)
            attack_type_considered = key[3:]
            # We need the total count of attack occurrences, so I sum the relevant values
            yield f"q2_number_of_{attack_type_considered}_attacks", sum(attack_occurrences)


        if key.startswith("q3_"):
            affected_users = list(values)
            industry_considered = key[3:]
            # We need the total count of affected users, so I sum the relevant values
            yield f"q3_total_affected_users_in_{industry_considered}", sum(affected_users)

        if key.startswith("q4_"):
            zero_day_attack_occurrences = list(values)
            country_considered = key[3:]
            # We need the total count of zero-day attacks, so I sum the relevant values
            yield f"q4_zero_day_attacks_in_{country_considered}", sum(zero_day_attack_occurrences)

        if key.startswith("q5_"):
            resolution_times = list(values)
            country_considered = key[3:]
            # We need the minimum resolution time for every considered country, so I only take the min of the combined values
            yield f"q5_minimum_resolution_time_in_{country_considered}", min(resolution_times)

if __name__ == "__main__":
    CyberThreadAnalyzer.run()



# ### RESULTS ### 
### Question 1 ###
# "q1_total_fin_loss_for_2021"    15873.40999999999
# "q1_total_fin_loss_for_2022"    15870.859999999999
# "q1_total_fin_loss_for_2023"    15958.080000000002
# "q1_total_fin_loss_for_2024"    15434.289999999999
# "q1_total_fin_loss_for_2018"    14720.480000000003
# "q1_total_fin_loss_for_2019"    13134.690000000006
# "q1_total_fin_loss_for_2020"    15767.950000000003
# "q1_total_fin_loss_for_2015"    14510.210000000001
# "q1_total_fin_loss_for_2016"    13947.260000000006
# "q1_total_fin_loss_for_2017"    16261.680000000004

### Question 2 ###
# "q2_number_of_Ransomware_attacks"       493
# "q2_number_of_SQL Injection_attacks"    503
# "q2_number_of_DDoS_attacks"     531
# "q2_number_of_Malware_attacks"  485
# "q2_number_of_Man-in-the-Middle_attacks"        459
# "q2_number_of_Phishing_attacks" 529

### Question 3 ###
# "q3_total_affected_users_in_IT" 250094829
# "q3_total_affected_users_in_Retail"     206776386
# "q3_total_affected_users_in_Banking"    225098406
# "q3_total_affected_users_in_Education"  215004732
# "q3_total_affected_users_in_Government" 201239030
# "q3_total_affected_users_in_Healthcare" 216271916
# "q3_total_affected_users_in_Telecommunications" 199567110

### Question 4 ###
# "q4_zero_day_attacks_in_Brazil" 85
# "q4_zero_day_attacks_in_China"  78
# "q4_zero_day_attacks_in_France" 89
# "q4_zero_day_attacks_in_Germany"        71
# "q4_zero_day_attacks_in_India"  83
# "q4_zero_day_attacks_in_Japan"  84
# "q4_zero_day_attacks_in_Russia" 69
# "q4_zero_day_attacks_in_UK"     76
# "q4_zero_day_attacks_in_USA"    71
# "q4_zero_day_attacks_in_Australia"      79

### Question 5 ###
# "q5_minimum_resolution_time_in_Brazil"  1
# "q5_minimum_resolution_time_in_China"   1
# "q5_minimum_resolution_time_in_France"  2
# "q5_minimum_resolution_time_in_Germany" 1
# "q5_minimum_resolution_time_in_India"   1
# "q5_minimum_resolution_time_in_Japan"   1
# "q5_minimum_resolution_time_in_Russia"  1
# "q5_minimum_resolution_time_in_UK"      1
# "q5_minimum_resolution_time_in_USA"     1
# "q5_minimum_resolution_time_in_Australia"       1