// Python dependencies

\d .ap

np:.p.import[`numpy]

// convert a pandas dataframe to a q table (this is a copy of .ml.df2tab)
/* df      = a pandas dataframe as a foreign
/. returns = a q table
dataframeToTable:{[df]
  df[`:reset_index]pykwargs `inplace`drop!11b;
  v:{flip(`$x[`:columns][`:tolist][]`)!(np[`:transpose]x[`:values])`};
  dt_cols:`$(.p.list df[`:select_dtypes][`datetime][`:keys][])`;
  t:v df[`:astype][dt_cols!count[dt_cols]#`int64;pykw[`copy;0b]];
  t[dt_cols]:"p"$t[dt_cols]+1970.01.01D00:00:00.000;
  t
  }

// converts q dates/times to numpy datetime64[ns]
/* x       = pmdznuvt
/. returns = foreign object
numpifyDatetime:{
 (@[;`.]np[`:array][;"datetime64[ns]"]@)each("p"$@[4#+["d"$0];-16+type x]x)-"p"$1970.01m;
 }


// convert a q table to pandas dataframe (this is just a copy of .ml.tab2df)
/* x       = q table
/. returns = a pandas dataframe as a foreign object
tableToDataframe:{
  fndcols:{m[`c]where(m:0!meta x)[`t]in y};
  r:.p.import[`pandas;`:DataFrame;@[flip 0!x;fndcols[x]"pmdznuvt";numpifyDatetime]][@;cols x];
  $[count k:keys x;r[`:set_index]k;r]
  }

