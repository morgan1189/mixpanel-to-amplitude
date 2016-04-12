# mixpanel-to-amplitude
This is a small Python script helping those in need to transfer their data from Mixpanel to Amplitude.

## Description
This script essentially uses the `export` and `engage` Mixpanel endpoints to extract the data, convert it to the Amplitude format, enrich the `user_properties` with what it could have obtained from `engage` of Mixpanel and then pour it into the `httpapi` endpoint of Amplitude. It also boasts multithreading support by breaking down the date interval into as many chunks as `THREAD_NUMBER` specifies.

## Usage
If you just want to see how this works for your case, all you need to do is to set up your personal Mixpanel and Amplitude keys and you should be able to run the script.
``` python
MIXPANEL_API_KEY = ...
MIXPANEL_API_SECRET = ...
AMPLITUDE_API_KEY = ...
```

However, note that if your Mixpanel project has a timezone different from UTC, you should also its time offset:
``` python
MIXPANEL_TIME_OFFSET = 3 # as in GMT+3 for Moscow
```

You can also control how much data you would like exported by setting the `FROM_DATE` and `TO_DATE` parameters. The second is optional and is assumed to be NOW by default.

## Features
- Multithreading support (fiddle with `THREAD_NUMBER` to enable/disable/modify)
- Custom user identification support (use `MIXPANEL_USER_ID_KEY` which will be searched for both in people and event properties and set as `user_id` for Amplitude event)

## Known issues
- No Revenue events transfer yet
- Android-devices have not been well tested
- Not all side-cases have been very well worked through
- I am not a pro Python developer (yet)

With the latter point in mind, please feel free to contribute to this project.

## Links
Mixpanel API:
https://mixpanel.com/docs/api-documentation/exporting-raw-data-you-inserted-into-mixpanel
https://mixpanel.com/docs/api-documentation/data-export-api#engage-default

Amplitude API:
https://amplitude.zendesk.com/hc/en-us/articles/204771828-HTTP-API
