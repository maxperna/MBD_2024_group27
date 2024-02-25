import re
from statistics import variance
from datetime import datetime

"""Function to parse a clock instant string into a datetime object
@type time_str: str
@param time_str: textual clock instant to parse
@return: a datetime object
"""
def parse_clock(time_str):
    return datetime.strptime(time_str, '%H:%M:%S')

"""Return the differnce in seconds of two consecutives time instants
@type clock_list: list
@param clock_list: list of clock instance
@returns: a list of delta in seconds
"""
def sec_difference(clock_list):
    delta_list = []
    
    for i in range(1,len(clock_list)):
        difference = abs(clock_list[i] - clock_list[i-1])
        delta_list.append(difference.total_seconds())
    
    return delta_list 

"""Function to calculate the variance
@type time_list: list
@param time_list: contains a list of time of whom calculate the variance
@returns: a variance
"""
def get_variance(time_list):
    try:
        return variance(time_list)
    except:
        0

"""Function to analize a game
@type game: str
@param game: textual file containing the annotated game
@type white: bool
@param white: analyze only the white clock if true (default True)
@param both: analyze both players
"""
def time_exctractor(game,white = True,both=False):
    clock_pattern = re.compile(r'%clk (\d+:\d+:\d+)')

    matches = re.finditer(clock_pattern, game)

    #Creating time list, adding a 0 at the beginning to perfrom subtraction after
    times = [parse_clock(match.group(1)) for match in matches]
    delta_list = sec_difference(times)

    if not(both):
        variance = get_variance(delta_list[int(not(white))::2])
    else:
        variance =get_variance(delta_list)

    return variance

