
def merged_dict(dict_prefix_exclusions_list):
    """
    merge arbitrary number of dicts and return a new dict
    use single dict, to make a copy
    """
    dict_merged = {}
    for dict_i, prefix_i, exclusions_i in zip(dict_prefix_exclusions_list):
        exclusions_i = set() if not exclusions_i else set(exclusions_i)
        for key, val in dict_i.items():
            if key not in exclusions_i:
                merged_key = key if not prefix_i else '%s_%s' % (prefix_i, key)
                dict_merged[merged_key] = val
    return dict_merged


def flattened_events(event):
    cb_data = event.get('edata', {}).get('cb_data', {})
    # TO-MERGE: cb_data, prefix=None, exclude=['users']
    cb_data_users = cb_data.get('users', [])
    for cb_data_user in cb_data_users:
        # TO-MERGE: cb_data_user, prefix='users', exclude=['roleCompetencyList']
        role_competency_list = cb_data_user['roleCompetencyList']
        for role_competency in role_competency_list:
            # TO-MERGE: role_competency, prefix='users_roleCompetencyList', exclude=['roleDetails','competencyDetails']
            # exclude=['roleDetails','competencyDetails'] results in no fields from this level being merged

            competency_details = role_competency['competencyDetails']
            for competency in competency_details:
                # TO-MERGE: competency, prefix='users_roleCompetencyList_competencyDetails', exclude=[]
                yield merged_dict([
                    # dict              prefix                                          exclusion
                    (cb_data,           None,                                           ['users']),
                    (cb_data_user,      'users',                                        ['roleCompetencyList']),
                    (role_competency,   'users_roleCompetencyList',                     ['roleDetails', 'competencyDetails']),
                    (competency,        'users_roleCompetencyList_competencyDetails',   []),
                ])

            role_details = role_competency['roleDetails']
            # TO-MERGE: role_details, prefix='users_roleCompetencyList_roleDetails', exclude=['childNodes']
            role_child_nodes = role_details['childNodes']
            for child_node in role_child_nodes:
                # TO-MERGE: child_node, prefix='users_roleCompetencyList_roleDetails_childNodes', exclude=[]
                yield merged_dict([
                    # dict              prefix                                              exclusion
                    (cb_data,           None,                                               ['users']),
                    (cb_data_user,      'users',                                            ['roleCompetencyList']),
                    (role_competency,   'users_roleCompetencyList',                         ['roleDetails', 'competencyDetails']),
                    (role_details,      'users_roleCompetencyList_roleDetails',             ['childNodes']),
                    (child_node,        'users_roleCompetencyList_roleDetails_childNodes',  []),
                ])
