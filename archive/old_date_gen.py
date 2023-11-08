import humanfirst
import numpy
import datetime

def get_list_utterance_datetimes(start_date: datetime.datetime, number_of_days: int, max_seconds_per_utterance: int, seed_string: str, size: int) -> list:
    '''
    Return a reproducible list of utterance datetimes for a conversation
    Conversations start at random time within the number of days from the start_date
    Utterances are spread between 0 and max seconds per utterance from the start time
    '''
    seconds_in_range = 60*60*24*number_of_days
    seed_int = int(humanfirst.objects.hash_string(seed_string,None),16)
    rng = numpy.random.RandomState(numpy.random.MT19937(numpy.random.SeedSequence(seed_int)))
    start_date_delta_seconds = rng.randint(0,seconds_in_range)
    convo_start_date = start_date + datetime.timedelta(seconds=start_date_delta_seconds)
    running_seconds = 0
    output_dates = [convo_start_date]
    for i in range(size - 1):
        running_seconds = running_seconds + rng.randint(0,max_seconds_per_utterance)
        output_dates.append(convo_start_date + datetime.timedelta(seconds=running_seconds))
    return output_dates