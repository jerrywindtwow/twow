import math
import pandas as pd


def separate_to_groups(par_df_first, par_col_name, par_group_num):
    par_df = par_df_first
    cols = par_df_first.columns
    grouped = pd.DataFrame(index=[], columns=cols)
    str_where = 'group==' + str(par_group_num)

    for group_id in range(1, par_group_num):
        par_df = par_df.query(str_where)
        data_sum = par_df[par_col_name].sum()
        amount = par_df.shape[0]
        ave = math.ceil(data_sum / (par_group_num - group_id + 1))
        # print(par_df[par_col_name])
        # print("avg=",ave)

        remain = ave
        abs_diff = data_sum
        abs_diff_index = -1

        for j in range(amount-1, -1, -1):
            elem_df = par_df[j:j+1]
            elem_index = elem_df.index[0]
            elem = par_df.at[elem_index, 'size']
            tmp = remain - elem

            if elem >= ave:
                par_df.at[elem_index, 'group'] = group_id
                par_df_first.at[elem_index, 'group'] = group_id
                grouped = grouped.append(elem_df)
                abs_diff = data_sum
                abs_diff_index = -1
                break
            elif tmp >= 0:
                remain = tmp
                par_df.at[elem_index, 'group'] = group_id
                par_df_first.at[elem_index, 'group'] = group_id
                grouped = grouped.append(elem_df)
                abs_diff = data_sum
                abs_diff_index = -1
            elif abs(tmp) < abs_diff:
                abs_diff = abs(tmp)
                abs_diff_index = elem_index

        if remain>abs_diff:
            par_df.at[abs_diff_index, 'group'] = group_id
            par_df_first.at[abs_diff_index, 'group'] = group_id
            elem_df = par_df.loc[abs_diff_index]
            grouped = grouped.append(elem_df)

    par_df=par_df.query(str_where)
    grouped = grouped.append(par_df)

    return grouped
