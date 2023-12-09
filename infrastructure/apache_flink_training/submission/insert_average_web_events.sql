insert into average_web_events
select host, AVG(num_hits)
from processed_events_aggregated 
group by host