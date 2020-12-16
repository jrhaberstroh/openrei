#!/Users/jhaberstroh/anaconda3/bin/python

import collections
import pandas as pd
import numpy as np
import sys

# ------- Load the input and check for basic validity 
if len(sys.argv) != 2:
  raise ValueError("investment_calc.py [file]")

with open(sys.argv[1]) as f:
  commands = [[i+1] + l.strip().split(' ') for i, l in enumerate(f)]

# -------- Define core functions
def p2f(x):
  return float(x.strip('%'))/100

def s_in_col(s, col):
  if len(col) == 0:
    return False
  return col.str.match(s).any()

class MonthYear(tuple):
  def __new__(self, month_or_monthyear, year=None):
    if type(month_or_monthyear) == str:
      if not year is None:
        raise ValueError("Cannot pass `year` second argument with string first argument")
      s_monthyear = month_or_monthyear
      has_dash = '-' in s_monthyear
      has_slash = '/' in s_monthyear
      if (has_dash and has_slash):
        ValueError(s_monthyear + " not recognized as MonthYear")
      if (not has_dash and not has_slash):
        ValueError(s_monthyear + " not recognized as MonthYear")
      if has_dash:
        l_monthyear = s_monthyear.split('/')
        if len(l_monthyear) != 2:
          ValueError(s_monthyear + " not recognized as MonthYear")
        year = l_monthyear[0]
        month = l_monthyear[1]
      if has_slash:
        l_monthyear = s_monthyear.split('/')
        if len(l_monthyear) != 2:
          ValueError(s_monthyear + " not recognized as MonthYear")
        year = l_monthyear[1]
        month = l_monthyear[0]
      year = int(year)
      month = int(month)
    elif type(month_or_monthyear) == int:
      if year is None:
        raise ValueError("Year argument must be passed with int first argument")
      month = month_or_monthyear
    else:
      raise ValueError("Constructor args must be month + year integers or monthyear string")
    if (month > 12 or month < 1):
      raise ValueError("Month cannot be outside of the range 1-12:" +
                       " received {}".format(month))
    return super(MonthYear, self).__new__(self, (year, month))
    return super(MonthYear, self).__new__(self, (year, month))
  def next_month(self):
    next_month = self[1] + 1
    next_year  = self[0]
    if next_month > 12:
      next_month -= 12
      next_year  += 1
    return MonthYear(next_month, next_year)

class MYIterator(collections.Iterator):
  def __init__(self, start_month, start_year):
    self.monthyear = MonthYear(start_month, start_year)
  def __next__(self):
    return_monthyear = self.monthyear
    self.monthyear = self.monthyear.next_month()
    return return_monthyear


class AccountingManager():
  def __init__(self):
    self.items = {}
    self.items['expense'] = {'now':[], 'monthly':{}}

  def push(self, item_type, item_args):
    full_valid_push=[x for x in filter(lambda x: x.startswith('push_'), dir(self))]
    suff_valid_push=[x.split('_',1)[1] for x in full_valid_push]
    if item_type in suff_valid_push:
      eval('self.push_{}(item_args)'.format(item_type))
    else:
      raise ValueError("Invalid item type: {}".format(item_type))

  def eval(self, mo):
    cashflows = np.zeros(len(self.items.keys()))
    for i, item in enumerate(self.items.keys()):
      cashflows[i] = eval('self.eval_{}(mo)'.format(item))
    return(sum(cashflows))

  def push_rent(self, item_args):
    if item_args[0] == 'new':
      if not 'rent' in self.items.keys():
        self.items['rent'] = pd.DataFrame({'unit':[], 'rent':[], 'newlease':[]})
      new_unit = item_args[1]
      new_rent = int(item_args[2])
      new_row = pd.DataFrame({'unit':[new_unit], 'rent':[new_rent], 'newlease':[True]})
      if s_in_col(new_unit, self.items['rent'].unit):
        raise ValueError("{} already being tracked.")
      self.items['rent'] = self.items['rent'].append(new_row)
    elif item_args[0] == 'update':
      up_unit = item_args[1]
      up_rent = int(item_args[2])
      if not 'rent' in self.items.keys():
        raise ValueError("No rent to be updated.")
      if not self.items['rent'].unit.str.match(up_unit).any():
        raise ValueError("{} not found in rent roll".format(item_args[1]))
      self.items['rent'] = self.items['rent'].query('unit != "{}"'.format(up_unit))
      up_row = pd.DataFrame({'unit':[up_unit], 'rent':[up_rent], 'newlease':[False]})
      self.items['rent'] = self.items['rent'].append(up_row)
    elif item_args[0] == 'rm':
      rm_unit = item_args[1]
      if not 'rent' in self.items.keys():
        raise ValueError("No rent to be removed.")
      if not self.items['rent'].unit.str.match(rm_unit).any():
        raise ValueError("{} not found in rent roll".format(item_args[1]))
      self.items['rent'] = self.items['rent'].query('unit != "{}"'.format(rm_unit))
    else:
      raise ValueError("rent argument not recognized: " + item_args[0]) 

  def eval_rent(self, mo):
    if not 'pm' in self.items.keys():
      self.items['rent'].newlease = False
    return(self.items['rent'].rent.sum())
  
  def push_pm(self, item_args):
    if item_args[0] == 'new':
      if not 'pm' in self.items.keys():
        self.items['pm'] = pd.DataFrame({'unit':[], 'leasefee':[], 'mgmtfee':[]})
      mgmtfee=p2f(item_args[1])
      leasefee=p2f(item_args[2])
      units=item_args[3:]
      new_pm_rows=pd.DataFrame({'unit':units, 
                                'leasefee':[leasefee]*len(units), 
                                'mgmtfee':[mgmtfee]*len(units)})
      self.items['pm'] = self.items['pm'].append(new_pm_rows)
    else:
      raise ValueError("pm argument not recognized: " + item_args[0]) 
   
  def eval_pm(self, mo):
    mgmt = 0
    lease = 0
    if 'rent' in self.items.keys():
      pm = self.items['pm']
      rent = self.items['rent']
      fees = pd.merge(pm, rent, on='unit', how='inner')
      fees = fees.assign(cost_mgmt  = fees.rent * fees.mgmtfee)
      fees = fees.assign(cost_lease = fees.rent * fees.leasefee * fees.newlease)
      mgmt  = sum(fees.cost_mgmt)
      lease = sum(fees.cost_lease)
      self.items['rent'].newlease = False
    return(- (mgmt + lease))

  def push_expense(self, item_args):
    action = item_args[0]
    if action == 'now':
      new_expense = float(item_args[2])
      self.items['expense']['now'].append(new_expense)
    elif action == 'monthly':
      new_exp = item_args[1]
      new_amt = float(item_args[2])
      self.items['expense']['monthly'][new_exp] = new_amt
    else:
      raise ValueError("expense action not recognized: " + action)
  
  def eval_expense(self, mo):
    total_expense = sum(self.items['expense']['now']) + sum(self.items['expense']['monthly'].values())
    self.items['expense']['now'] = []
    return(-total_expense)

  def push_loan(self, item_args):
    if (item_args[0] == 'new'):
      if not 'loan' in self.items.keys():
        self.items['loan'] = pd.DataFrame({'name':[],
                                           'initial_balance' : [],
                                           'balance' : [], 
                                           'm_term' : [],
                                           'y_rate' : [], 
                                           'm_pmt' : [],
                                           'm_fee_usd' : [],
                                           'm_fee_pct' : []})
      name = item_args[1]
      if self.items['loan'].shape[0] > 0 and self.items['loan'].name.str.match(name).any():
        raise ValueError("Loan already exists: "+ name)

      principal = float(item_args[2])
      term = float(item_args[3]) * 12
      y_r   = p2f(item_args[4]) 
      r   = y_r / 12
      pmi = p2f(item_args[5]) / 12
      self.items['expense']['now'].append(-principal)
      amort_pmt = principal * r * (1 + r)**(term) / ((1 + r)**term - 1)
      fee_pmt   = principal * pmi
      new_mortgage = pd.DataFrame({'name': [name],
                                   'initial_balance' : [principal],
                                   'balance' : [principal], 
                                   'm_term' : [term],
                                   'y_rate' : [y_r], 
                                   'm_rate' : [r], 
                                   'm_pmt' : [amort_pmt],
                                   'm_fee_usd' : [principal * pmi],
                                   'm_fee_pct' : [0]})

      self.items['loan'] = self.items['loan'].append(new_mortgage)
      self.items['loan'].set_index(np.arange(self.items['loan'].shape[0]), inplace = True)
    elif item_args[0] == 'payoff':
      name = item_args[1]
      pay_all=len(item_args) < 3
      if not s_in_col(name, self.items['loan'].name):
        raise ValueError("Loan does not exist: "+ name)
      match_row = self.items['loan'].index[self.items['loan'].name == name]
      assert(len(match_row) == 1)
      balance = self.items['loan'].balance[match_row].values
      if pay_all:
        self.items['expense']['now'].append(balance)
        self.items['loan'] = self.items['loan'].query('name != "{}"'.format(name))
      else:
        pay_amount = float(item_args[2])
        if (pay_amount > balance):
          raise ValueError("Payoff amount exceeds balance. Remove amount to do a full payoff.")
        new_balance = balance - pay_amount
        self.items['expense']['now'].append(pay_amount)
        self.items['loan'].set_value(match_row, 'balance', new_balance)
    else:
      raise ValueError("loan action not recognized: " + item_args[0])

  def eval_loan(self, mo):
    loans = self.items['loan'] 
    loans = loans.assign(full_pmt = loans.m_pmt + loans.m_fee_usd + loans.m_fee_pct * loans.balance,
                         paydown = loans.m_pmt - loans.m_rate * loans.balance)
    
    loans = loans.assign(balance = loans.balance - loans.paydown)
    cashflow = loans.full_pmt.sum()
    self.items['loan'] = loans.drop(['full_pmt', 'paydown'], axis=1)
    return(-cashflow)

# ------- Prepare the commands
commands = [[c[0]] + [MonthYear(c[1])] + c[2:] for c in commands]
commands = collections.deque(sorted(commands, key=lambda x: (x[1], x[0])))
if (len(commands) == 0): raise ValueError("No instructions found!")

# ------- Calculate cashflows 
am = AccountingManager()
i = 0
num_months = 12
mo_monthyear = [None] * num_months
mo_cashflow  = np.zeros(num_months)
next_event = commands.popleft()

# TODO: allow custom start date
for mo in MYIterator(11, 2018):
  mo_monthyear[i] = mo
  while not next_event is None and (next_event[1] == mo):
    try: 
      am.push(next_event[2], next_event[3:])
    except:
      print("On input line {}:".format(next_event[0]))
      raise
    try:
      next_event = commands.popleft()
    except IndexError:
      next_event = None
  mo_cashflow[i] = am.eval(mo)
  i += 1
  if i >= num_months:
    break

summary = pd.DataFrame({'date':  mo_monthyear, 'cashflow': mo_cashflow})
print(summary)
