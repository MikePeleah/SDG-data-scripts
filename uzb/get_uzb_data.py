import sys
import re
import os
import json
import time
import requests
from bs4 import BeautifulSoup


def progress_bar(done, total, l=10, short = False):
    pdone = done / total
    a = int(pdone * l)
    b = l - a 
    if short:
        return "[%s%s] %4.1f%%" % (u'\u2588'*a, u'\u2591'*b, pdone*100)
    else:
        return "%d done out of %d, %4.1f%% done [%s%s]" % (done, total, pdone*100, u'\u2588'*a, u'\u2591'*b)


def try_get(url, ntries=3, delay=10, verbose = False):
    """
    Try to get url for ntries, after each unsuccessful attempt delay for delay seconds
    return {'code': result code, "Ok" if Ok, "Error" else
            'result': result of get}
    v2 for easy handling response
        try_result = try_get(urls[i], ntries=3, delay=10, verbose = False)
        response = try_result['result']
    """
    attempts = ntries
    while attempts>0:
        attempts_try = attempts
        if verbose:
            print("%d attemps left, trying %s" % (attempts, url))
        try:
            result = requests.get(url)
            code = "Ok"
            attempts = 0
            result.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            attempts = 0
            code = 'Error'
            result = {'status_code': 'Http Error', 'content' : 'Http Error: %s' % errh}
            if verbose:
                  print (result)
        except requests.exceptions.ConnectionError as errc:
            attempts = attempts_try - 1
            code = "Error"
            result = {'status_code': 'Error Connecting', 'content' : 'Error Connecting: %s' % errc}
            if verbose:
                  print (result)
            time.sleep(delay)
        except requests.exceptions.Timeout as errt:
            attempts = attempts_try - 1
            code = "Error"
            result = {'status_code': 'Timeout Error', 'content' : 'Timeout Error: %s' % errt}
            if verbose:
                  print (result)
            time.sleep(delay)
        except requests.exceptions.RequestException as err:
            attempts = 0
            code = "Error"
            if verbose:
                  print (result)
            result = {'status_code': 'Other Error %s' % err, 'content' : 'Oops: Something Else %s' % err}
        if verbose:
            print("  ",code, result if code=="Error" else "")
    return {'code': code, 'result': result}

# . . . __main__  . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . .
output_dir = "C:\\temp\\uzb"
current_dir = os.getcwd()

langs = ['en', 'ru', 'uz']
goals = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]

total_task = len(langs) * len(goals)
done_task = 0
# NB: JSON Beutifier https://www.labnol.org/beautifier/

for lang in langs:
    os.makedirs(os.path.join(output_dir, lang), exist_ok=True)
    for goal in goals:
        # get goals info https://nsdg.stat.uz/api/databanks/get-data-banks/?id={goal}}&lang={lang}
        try_result = try_get(url = f"https://nsdg.stat.uz/api/databanks/get-data-banks/?id={goal}&lang={lang}")
        response = try_result['result']
        if try_result['code'] != 'Ok':
            print(f"- Error getting Goal {goal}-{lang}. Error: {response['content']}\n")
        else:
            goal_info = json.loads(response.content)
            with open(os.path.join(output_dir, lang, f'goal {goal}.json'), 'w', encoding = 'utf-8') as f:
                json.dump(goal_info, f)
            # Now loop through indiacators in goal info
            all_indicators = goal_info['all_indicators']
            for indicator in all_indicators:
                indicator_id = indicator['indicator_id']
                # get indicators info https://nsdg.stat.uz/api/databanks/get-indicator-table/?id={indicator_id}}&lang={lang}
                try_result = try_get(url = f"https://nsdg.stat.uz/api/databanks/get-indicator-table/?id={indicator_id}&lang={lang}")
                response = try_result['result']
                if try_result['code'] != 'Ok':
                    print(f"- Error getting indicator {indicator_id}-{lang}. Error: {response['content']}\n")
                else:
                    indicator_info =  json.loads(response.content)
                    with open(os.path.join(output_dir, lang, f'indicator {indicator_id}.json'), 'w', encoding = 'utf-8') as f:
                        json.dump(indicator_info, f)
        done_task = done_task + 1
        print(f"{progress_bar(done_task, total_task, l=15, short = True)}  Processed {goal} - {lang}")


print("* * * That's all, folks! * * *")


