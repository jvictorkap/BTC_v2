import sys

sys.path.append("..")

import os
from datetime import datetime, timedelta, date
import workdays
import numpy as np
import pandas as pd

side_bol={
    'T':1,
    'D':-1
}

def parse_excel_safra(file_path):
    df = pd.read_excel(file_path)

    df.rename(
        columns={
            "fundo": "str_fundo",
            "corretora": "str_corretora",
            "vencimento": "dte_datavencimento",
            "ativo": "str_papel",
            "quantidade": "dbl_quantidade",
            "taxa": "dbl_taxa",
            'lado':'str_tipo'
        },
        inplace=True,
    )
    df["str_tipo_registro"] = df["modalidade"].apply(
        lambda x: "R" if "B" in x.upper()  else "N" 
    )
    df["str_modalidade"] = df["str_tipo_registro"].apply(
        lambda x: "E1" if x == "N" else None
    )
    
    df["str_tipo_comissao"] = "A"
    df["dbl_valor_fixo_comissao"] = 0
    df["str_reversivel"] = "TD"
    df['str_corretora']='Safra'

    df["dte_databoleta"] = workdays.workday(
        date.today(), 0, workdays.load_holidays()
    ).strftime("%d/%m/%Y")
    df["str_status"] = "Emprestimo"
    df['str_tipo'] = df['str_tipo'].apply(lambda x: x[0])
    df['dbl_quantidade']=df.apply(lambda row: 1*abs(row['dbl_quantidade'])*side_bol[row['str_tipo']],axis=1)

    return df[
        [
            "str_fundo",
            "str_corretora",
            "str_tipo",
            "dte_datavencimento",
            "dbl_taxa",
            "str_reversivel",
            "str_tipo_registro",
            "str_modalidade",
            "str_tipo_comissao",
            "dbl_valor_fixo_comissao",
            "str_papel",
            "dbl_quantidade",
            "str_status",
        ]
    ]