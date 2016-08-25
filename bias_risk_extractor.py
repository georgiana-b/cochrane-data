# -*- coding: utf-8 -*-
import os
import csv
import pandas
import json
from multiprocess import Pool
from copy import deepcopy
try:
    from lxml import etree
except ImportError:
    import xml.etree.ElementTree as etree

REVIEWS_DIR='../ALL-2015-06'


def extract_review_info(file_path):
    """Extract study identifiers and results"""
    tree = etree.parse(file_path)
    study_robs = []
    studies = []

    # Find out NTC******** ids in the XMLs. Turns out they appear as study_id
    # xpatheval = etree.XPathEvaluator(tree)
    # matches = xpatheval("//*[contains(*,'NCT')]")
    # for match in matches:
    #     print(file_path)
    #     print(match.tag)
    #     print(match.attrib)
    #     print(match.text)

    quality_item_data_entries = tree.findall('//QUALITY_ITEM_DATA_ENTRY')

    # Get risk of bias
    for quality_item_data_entry in quality_item_data_entries:
        # Get the results from a QUALITY_ITEM_DATA_ENTRY element
        study_rob = {}
        study_rob['study_id'] = quality_item_data_entry.attrib['STUDY_ID']
        study_rob['modified'] = quality_item_data_entry.attrib.get('MODIFIED', '')
        study_rob['result'] = quality_item_data_entry.attrib['RESULT']
        study_rob['group_id'] = quality_item_data_entry.attrib.get('GROUP_ID', '')
        study_rob['group_name'] = ''
        for description in quality_item_data_entry.iter('P'):
            study_rob['result_description'] = description.text

        # Get info about the rob from the parent QUALITY_ITEM element
        quality_item = quality_item_data_entry.getparent().getparent()
        study_rob['rob_id'] = quality_item.attrib['ID']
        study_rob['rob_name'] = quality_item.findtext('NAME')
        rob_description = quality_item.find('DESCRIPTION/P')
        study_rob['rob_description'] = rob_description.text
        for group in quality_item.iter('QUALITY_ITEM_DATA_ENTRY_GROUP'):
            group_id = group.attrib.get('ID')
            if group_id == study_rob['group_id']:
                study_rob['group_name'] = group.findtext('NAME')
        study_robs.append(study_rob) 

    included_studies = tree.find('//INCLUDED_STUDIES')

    #Get references
    for study in included_studies.iter('STUDY'):
        study_info = {}
        study_info['file'] = file_path
        study_info['id'] = study.attrib['ID']
        corresponding_robs = [rob for rob in study_robs
                              if rob['study_id'] == study_info['id']]
        study_info['robs'] = corresponding_robs
        study_info['study_type'] = study.attrib['DATA_SOURCE']
        study_info['references'] = []
        for reference in study.iter('REFERENCE'):
            ref = {}
            ref['type'] = reference.attrib['TYPE']
            ref['authors'] = reference.findtext('AU') or ''
            ref['title'] = reference.findtext('TI') or ''
            ref['source'] = reference.findtext('SO') or ''
            ref['year'] = reference.findtext('YR') or ''
            ref['vl'] = reference.findtext('VL') or ''
            ref['no'] = reference.findtext('NO') or ''
            ref['pg'] = reference.findtext('PG') or ''
            ref['country'] = reference.findtext('CY') or ''
            ref['identifiers'] = []
            for identifier in reference.iter('IDENTIFIER'):
                ident = deepcopy(identifier.attrib)
                ident = { key.lower(): ident[key] for key in ident
                          if key not in ['MODIFIED', 'MODIFIED_BY'] }
                ref['identifiers'].append(ident)
            study_info['references'].append(ref)

        studies.append(study_info)
    return studies

def write_results_to_csv(filename, headers, result_key):
    with open(filename, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers, extrasaction='ignore', quoting=csv.QUOTE_ALL)

        writer.writeheader()

        for file_studies in all_studies:
            for study in file_studies:
                study_info = { key: study[key] for key in study
                            if key not in ['robs', 'references'] }
                for rob in study[result_key]:
                    rob.update(study_info)
                    writer.writerow(rob)

def convert_keys_to_string(dictionary):
    """Recursively converts dictionary keys to strings."""
    if not isinstance(dictionary, dict):
        return dictionary
    return dict((str(k), convert_keys_to_string(v))
        for k, v in dictionary.items())

if __name__ == '__main__':
    pool = Pool(processes=8)
    reviews = []
    for subdir, dirs, files in os.walk(REVIEWS_DIR):
        for file in files:
            filepath = os.path.join(subdir, file)
            if filepath.endswith('.rm5'):
                reviews.append(filepath)

    reviews = [filepath for filepath in reviews
                   if 'publication' in filepath]
    all_studies = pool.map(extract_review_info, reviews, chunksize=8)

    rob_headers = ['file', 'id', 'modified', 'result', 'result_description',
                   'rob_name', 'rob_id', 'rob_description', 'group_id', 'group_name']
    write_results_to_csv('robs.csv', rob_headers, 'robs')
    reference_headers = ['file', 'id', 'study_type', 'type', 'authors', 'title',
                        'source', 'year', 'vl', 'no', 'pg', 'country', 'identifiers']
    write_results_to_csv('references.csv', reference_headers, 'references')

    studies = pandas.DataFrame()
    for file_studies in all_studies:
        studies = studies.append(pandas.DataFrame(file_studies), ignore_index=True)
    print('Total nr. studies: {0}'.format(studies.count()))
    print('Unique studies by id: {0}'.format(studies['id'].nunique()))

    nr = 0
    ident = []
    for file_studies in all_studies:
        for study in file_studies:
            for reference in study['references']:
                if len(reference['identifiers']) > 0:
                    nr += 1
                    ident.append(study)

    # How many studies have at least 1 identifier
    print('Total nr. identifiers: {0}'.format(nr))
    studies_with_ident = list({v['id']:v for v in ident}.values())
    print('Number of studies that have at least 1 identifier: {0}'.format(len(studies_with_ident)))

    # Analyze robs
    robs = pandas.DataFrame()
    for file_studies in all_studies:
        for study in file_studies:
            robs = robs.append(pandas.DataFrame(study['robs']), ignore_index=True)

    robs_group = robs.groupby(['rob_id', 'group_id'])
    all_rob_names = robs_group.rob_name.unique()
    all_group_names = robs_group.group_name.unique()
    joined_groups = pandas.concat([all_rob_names, all_group_names], axis=1, join='inner')
    joined_groups.to_csv('rob_groups.csv')

    # Studies with multiple reviews
    studies_reviews_group = studies.groupby('id')
    no_multiple_reviews = 0
    for name, group in studies_reviews_group:
        reviews = group.file.unique()
        if len(reviews) > 1:
            no_multiple_reviews += 1
            all_study_robs = list(group.robs)
            study_robs = pandas.DataFrame(all_study_robs[0])
            grouped_robs = study_robs.groupby(['rob_id', 'rob_name', 'group_id', 'group_name'])
            for rob_name, rob_group in grouped_robs:
                results = rob_group.result.unique()
                # Check if there are multiple results for the same rob
                if len(results) > 1:
                    res = {name:{
                                'reviews': reviews.tolist(),
                                'results': rob_group.to_dict()
                                }
                          }
                    res = convert_keys_to_string(res)
                    studies_file_path = 'studies_with_mutiple_reviews.json'
                    existent_studies = {'studies': []}
                    if os.path.exists(studies_file_path):
                        with open(studies_file_path, 'r') as f:
                            existent_studies.update(json.loads(f.read()))

                    existent_studies['studies'].append(res)

                    with open(studies_file_path, 'w') as f:
                        f.write(json.dumps(existent_studies, indent=4))

    print('Number of studies with multiple reviews: {0}'.format(no_multiple_reviews))
