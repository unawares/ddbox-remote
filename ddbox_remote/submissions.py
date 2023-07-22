from typing import List, Dict
import requests
import os
from ddbox_remote.configs import BASE_API_URL
from ddbox_remote.utils import ensure_path


def submit_metrics_moses(smiles_list: List[str]):
    api_url = BASE_API_URL + '/submission/metrics/moses/'
    response = requests.post(api_url, json={
        'smiles_list': smiles_list
    })

    if response.status_code != 200:
        raise Exception(response.text)
    
    return response.json()['submission_id']


def submit_docking(smiles_list: List[str], receptors: List[dict]):
    api_url = BASE_API_URL + '/submission/docking/'

    response = requests.post(api_url, json={
        'smiles_list': smiles_list,
        'receptor_ids': [receptor['id'] for receptor in receptors],
        'centers': [receptor['center'] for receptor in receptors],
        'sizes': [receptor['size'] for receptor in receptors],
    })

    if response.status_code != 200:
        raise Exception(response.text)
    
    return response.json()['submission_id']


def submission_metrics_moses_status(submission_id: str):
    api_url = BASE_API_URL + '/submission/metrics/result/moses/'
    response = requests.get(api_url, params={
        'submission_id': submission_id,
    })

    if response.status_code != 200:
        raise Exception(response.text)

    return response.json()


def submission_docking_status(submission_id: str):
    api_url = BASE_API_URL + '/submission/result/docking/'
    response = requests.get(api_url, params={
        'submission_id': submission_id,
    })

    if response.status_code != 200:
        raise Exception(response.text)

    return response.json()


def download_docking_results(submission_id, out_dir='./submissions/docking/'):
    data = submission_docking_status(submission_id)
    if 'results' in data:
        submission_out_dir = os.path.join(out_dir, submission_id)
        for index, docking_result in enumerate(data['results']['vina']['docking']):
            submission_result_out_dir = os.path.join(submission_out_dir, str(index))
            ensure_path(submission_result_out_dir)

            with open(os.path.join(submission_result_out_dir, 'smiles.txt'), 'w', encoding='utf-8') as f:
                f.write(docking_result['smiles'])
            
            with open(os.path.join(submission_result_out_dir, 'receptor_id.txt'), 'w', encoding='utf-8') as f:
                f.write(docking_result['receptor_id'])

            with open(os.path.join(submission_result_out_dir, 'vina_result.pdbqt'), 'w', encoding='utf-8') as f:
                f.write(docking_result['vina_pdbqt'])

            with open(os.path.join(submission_result_out_dir, '%s_target.pdbqt' % docking_result['receptor_id']), 'w', encoding='utf-8') as f:
                f.write(docking_result['receptor_pdbqt'])

            with open(os.path.join(submission_result_out_dir, '%s_conf.txt' % docking_result['receptor_id']), 'w', encoding='utf-8') as f:
                f.write(docking_result['receptor_conf'])

            with open(os.path.join(submission_result_out_dir, 'ligand.pdbqt'), 'w', encoding='utf-8') as f:
                f.write(docking_result['ligand_pdbqt'])
    else:
        raise Exception("Results are not available: %s" % data)
