import os

os.environ['SUBJECTS_DIR']='/home/pmurphy/meg_data/surprise/MRIs/fs_converted'
from pymeg import source_reconstruction as pymegsr


subjects = {'DCB': 3}#,
#            'DHB': 3,
#            'ECB': 3,
#            'EMB': 3,
#            'EXF': 3,
#            'EXG': 3,
#            'GSB': 3,
#            'HBC': 3,
#            'JTB': 3,
#            'KSV': 3,
#            'NIF': 3,
#            'OMF': 3,
#            'PDP': 3,
#            'QNV': 4,
#            'TFD': 3,
#            'TNB': 3,
#            'TSJ': 3}


def submit_aggregates(cluster='uke'):
    from pymeg import parallel
    for subject, session in subjects.items():
        for sessnum in range(1,session+1):
            for datatype in ['F','BB']:
                parallel.pmap(aggregate, [(subject, sessnum, datatype)],
                      name='agg' + str(sessnum) + str(subject) + datatype,
                          tasks=6, memory=60, walltime='12:00:00', env="mne"
                      )
                return


def aggregate(subject, session, datatype):
    from pymeg import aggregate_sr as asr
    from os.path import join
    data = (
        '/home/pmurphy/Surprise_accumulation/Analysis/MEG/Conv2mne/%s-SESS%i-*%s*-lcmv.hdf' % (
            subject, session, datatype))
    labels = pymegsr.get_labels(subject)
    labels = pymegsr.labels_exclude(labels,
                                    exclude_filters=['wang2015atlas.IPS4',
                                                     'wang2015atlas.IPS5',
                                                     'wang2015atlas.SPL',
                                                     'JWDG_lat_Unknown'])
    clusters = {l.name:[l.name] for l in labels}
    if datatype == 'F':    # time-frequency
        agg = asr.aggregate_files(data, data, (-0.4, -0.2), to_decibels=True, all_clusters=clusters)
    elif datatype == 'BB':    # broadband
        agg = asr.aggregate_files(data, data, (-0.2, 0), to_decibels=False, all_clusters=clusters)

    filename = join(
        '/home/rbrink/NRS/agg/',
        'S%i_SESS%i_%s_agg.hdf' % (subject, session, datatype))
    asr.agg2hdf(agg, filename)


if __name__=="__main__":
    submit_aggregates()
