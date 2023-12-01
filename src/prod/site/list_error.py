""""
Здесь хранятся все найденные исключения, чтобы в процессе получения ошибки чтобы проще было обработать исключения
обратите внимание, что поиск осуществляется не по полной строке. а только по первым 37 символам.
"""


def get_key_dict37(string, dictionary):
    for key, v in dictionary.items():
        if v[:37] == string[:37]:
            return key


list_error = {
    '500': 'Something went wrong. Please try again or contact comtrade@un.org.',  # переработай запрос полностью
    '429': 'Rate limit is exceeded. Try again in 5 seconds.',
    '404': 'The resource you are looking for has been removed, had its name changed, or is temporarily unavailable.',
    '403': 'Access denied due to invalid subscription key. Make sure to provide a valid key for an active subscription.',
    'error_3': 'Out of call volume quota. Quota will be replenished in',
    # обрати внимание на запятые и или любые другие разделители в конце строки переменных их не должно быть
    'error_4': 'The field reporterCode is invalid',
}
