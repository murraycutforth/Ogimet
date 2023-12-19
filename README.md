# Ogimet

Based on the original repo at https://github.com/iuiuiu-wayy/Ogimet

I have created a CSV of all UK weather stations available on ogimet.

Running the script `download_all_scotland.py` then automatically locates all highland weather stations, and attempts to download daily weather data from all these stations from 2000 to 2021. I have mmade quite a few robustness improvements so if this fails for any reason then you can just restart the script and it will continue where it left off (partially filled folders for a particular month should be deleted manually).

Dependencies in requirements.txt
