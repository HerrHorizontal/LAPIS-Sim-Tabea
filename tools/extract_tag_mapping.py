import re
import os
import sys
import pprint

tag_value_template = re.compile("^\[lapis\-\d+.\d+\]")
tag_key_template = re.compile("^log:  week_\d\d_\d.\d+_\d.\d+_\d+_\d+_.+\.log$")
outfile_template = re.compile("^out")


path="/portal/ekpbms3/home/tfesenbecker/simulation/sim-2020-02-25/week_25_1.0_0" \
     ".0_16_480_default_pjr_no_jr/out.week_25_1.0_0.0_16_480_default_pjr_no_jr"
path2="/portal/ekpbms3/home/tfesenbecker/simulation/sim-2020-02-25/week_25_1.0_0.0_16_480_default_pjr_no_jr/"
path3="/portal/ekpbms3/home/tfesenbecker/simulation/sim-2020-02-25/"

destination_path="tagmapping_28-02-2020.txt"


def get_outfile_tag(file):
    tag_keys = []
    tag_values = []
    for i, line in enumerate(open(file)):
        for match in re.finditer(tag_key_template, line):
            tag_keys.append(match)
        for match in re.finditer(tag_value_template, line):
            tag_values.append(match)

    if len(tag_keys) != 1:
        print(tag_keys)

    tag_key = tag_keys[0][0][6:-4]
    tag_value = tag_values[0][0][1:-1]
    split = tag_key.split("_")
    tag_key_level1 = "_".join(split[:6] + split[8:])
    tag_key_level2 = "_".join(split[6:8])
    print(tag_key_level1, tag_key_level2, tag_value)

    return [tag_key_level1, tag_key_level2, tag_value]


def update_tag(mapping, tag_key_level1, tag_key_level2, tag_value):

    if tag_key_level1 in mapping.keys():
        if tag_key_level2 in mapping[tag_key_level1].keys():
            mapping[tag_key_level1].update({tag_key_level2: tag_value})
        else:
            mapping[tag_key_level1][tag_key_level2] = tag_value

    else:
        mapping[tag_key_level1] = {tag_key_level2: tag_value}

    return mapping


def process_tags(path):
    mapping = eval(eval(open(destination_path, 'r').read()))
    print(mapping, type(mapping))

    for subdir, dirs, files in os.walk(path):
        for file in files:
            if bool(outfile_template.match(file)):
                print(os.path.join(subdir, file))
                print(get_outfile_tag(subdir+"/"+file))
                mapping = update_tag(mapping, *get_outfile_tag(subdir+"/"+file))
    print(mapping)
    mapping = pprint.pformat(str(mapping), indent=2)
    open(destination_path, 'w').write(mapping)
    # destination_file = open(destination_path, "w")
    # destination_file.write(str(mapping))
    # destination_file.close()


process_tags(sys.argv[1])