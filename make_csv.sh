sed -e ':a' -e 'N' -e '$!ba' -e 's/:\nconcat\n/:/g' parsed_Y_Mar31_AL3.txt | sed -e ':a' -e 'N' -e '$!ba' -e 's/\n/, /g' | sed $'s/---/\\\n/g' > Desktop/parsed_Y_Mar31_AL3.csv 
