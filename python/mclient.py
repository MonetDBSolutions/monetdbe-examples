"""
(c) 2020 MonetDB Solutions All rights reserved
author: M.L. Kersten

This code module provides a version of mclient for MonetDB/e.
Its functionality is incrementally constructed.
"""
import time
import json
import monetdbe
import argparse


def usage():
    print("\\<file   - read input from file")
    print("\\>file   - save response in file, or stdout if no file is given")
    #  print("\\|cmd    - pipe result to process, or stop when no command is given")
    print("\\b       - read a cookbook of precooked SQL queries or show content")
    print("\\s       - display the Holmes schema ")
    #  print("\\history - show the readline history")
    #  print("\\help    - synopsis of the SQL syntax")
    #  print("\\D table - dumps the table, or the complete database if none given.")
    #  print("\\d[Stvsfn]+ [obj] - list database objects, or describe if obj given")
    #  print("\\A       - enable auto commit")
    #  print("\\a       - disable auto commit")
    #  print("\\e       - echo the query in sql formatting mode")
    print("\\t       - set the timer {none,clock,performance} (none is default)")
    print("\\f       - format rendering {default, csv, tab, raw, sql, pandas, line, keyvalue, html, json, rowcount}")
    print("\\w#      - set maximal screen width (-1=unlimited, 0=terminal width, >0=limit to num)")
    #  print("\\r#      - set maximum rows per page (-1=raw)")
    #  print("\\L file  - save client-server interaction")
    #  print("\\X       - trace mclient code")
    print("\\q       - terminate session and quit mclient")


timerformat = None
formatter = 'default'
screensize = 120
outputfile = None


def dummy():
    return [[1], [2], [4]], None

book = {
    "test": "select @",
    ".dummy": dummy,
    "count_tables": "select count(*) from sys.tables",
}


def loadbook(fname):
    try:
        f = open(fname,"r")
        clist = f.readlines()
        for c in clist:
            cmd, sql = c.split(':')
            book.update({cmd: sql})
    except IOError as msg:
        print(f"Failed to open cookbook '{fname}': {msg}")

def showbook():
    for e in book.keys():
        print("%-20s:%s" %(e, book[e]))
        #  print(f"{e} :{book[e]}")


typewidth= {
    'int8': 8,
    'int16': 16,
    'int32': 32,
    'oid': 16,
    'uuid': 36,
    'float64': 32,
    'double128':64
}

def layout(header, nw, nowrap=True):
    # calculate the preferred width of the columns using the old mclient approach
    #  print(f"Layout:{nw}")
    #  print(f"Target screen size:{screensize}")
    minvarcolsize = 10
    newsize = []
    hsize = []
    minvartotal = 0
    for h,s  in zip(header, nw):
        #  print(h.name, h.type_code)
        tname = str(h.type_code)
        flag = tname in ['int8', 'int16', 'int32', 'int64', 'oid', 'double', 'float64' ]
        if s == 0 and tname in typewidth:
            s = typewidth['tname']

        if nowrap or flag:
            minvartotal += s
        else:
            if s > minvarcolsize:
                minvartotal += minvarcolsize
            else:
                minvartotal += s
        # stop if the header becomes too large
        if screensize and screensize < sum(hsize):
            break
        hsize.append(len(h.name))
        newsize.append(s)
        nw = newsize

    # punish the column until you cannot squeeze any further
    overshoot = sum(nw) - screensize
    while screensize > 0 and sum(nw) > screensize and overshoot > 0:
        punish = int(overshoot / 10)
        if punish == 0:
            punish = 1
        maxwidth = max(nw)
        idx  = nw.index(maxwidth)
        if nw[idx] > punish and nw[idx] > hsize[idx]:
            nw[idx] -= punish
        overshoot -= punish

    #  print(f"Final:{newsize},{hsize}, {vartotal},{minvartotal}")
    return nw

def render(header, tuples, style='fancy', headeron=True, colsep='|', rowsep='\n', width=5, maxwidth=25, nullvalue=''):
    if not header and not tuples:
        return ""
    if not style:
        style = 'csv'
    #  print(f"formatter '{style}'")
    #  print('HDR', header)
    #  print('TUP', tuples)
    names = [h.name for h in header]
    answer = ''
    try:
        if style == 'sql':
            hdr = f'INSERT into "table"('
            sep = ''
            for d in names:
                hdr = hdr + sep + d
                sep = ','
            hdr = hdr + ') VALUES'
            for t in tuples:
                answer += f"{hdr}{t};\n"
            return answer

        if style == 'pandas':
            df = pd.DataFrame(tuples)
            pd.set_option('display.max_rows', 500)
            pd.set_option('display.max_columns', 500)
            pd.set_option('display.width', 1000)
            return df

        if style == 'list' or style == 'csv' or style == 'tabs':
            if headeron:
                answer += colsep.join(names) + rowsep
            for t in tuples:
                answer += colsep.join([str(e) for e in t]) + rowsep
            return answer

        if style == 'line':
            size = max([len(t) for t in names])
            for t in tuples:
                for h, e in zip(names, t):
                    answer += "{0:<{size}} = {1:}\n".format(h, e, size=size)
                answer += '\n'
            return answer

        if style == 'keyvalue':
            size = max([len(t[0]) for t in tuples])
            for t in tuples:
                answer += "{0:>{w}}: {1:}\n".format(t[0], t[1], w=size)
            return answer

        if style == 'raw':
            if not tuples:
                return 'No result set'
            for r in tuples:
                answer += (',\t'.join([str(e) for e in r])) + rowsep
            return answer

        if style == 'default':
            # create an adjusted column width which respects the screensize
            nw = []
            if tuples:
                nw = [len(str(h)) + 1 for h in names]
                for t in tuples[:250]:
                    for i in range(0, len(t)):
                        nw[i] = max(nw[i], len(str(t[i])))

            # nw = layout(header, nw, nowrap=(len(tuples) == 0))
            if headeron:
                hdr = ''
                newnames = []
                for h, w in zip(names, nw):
                    v = "{0:<{width}}".format(h, width=w)
                    if len(hdr) + len(v) > screensize:
                        hdr = hdr + '>more'
                        break
                    newnames.append(h)
                    if len(v) <= w:
                        v = v + ' ' * (w + 1 -len(v))
                    hdr = hdr + v
                names =  newnames
                answer += hdr + '\n'

                hdr = ''
                for h, w in zip(names, nw):
                    hdr = hdr + '-' * w + ' '
                answer += hdr + '\n'

            for t in tuples:
                row = ''
                leftover = []
                morerows = False
                for n, e, w in zip(names, t, nw):
                    if e is None:
                        e = nullvalue
                    val = str(e)[:w]
                    if type(e) == str:
                        v = "{0:<{width}} ".format(val, width=w)
                    else:
                        v = "{0:>{width}} ".format(val, width=w)
                    row = row + v
                    if len(val) < len(str(e)):
                        # repeat the lines with the remainders
                        leftover.append(str(e)[w:])
                        morerows = True
                    else:
                        leftover.append('')
                if row:
                    answer += row + '\n'
                while morerows:
                    newleftover = []
                    row = ''
                    for n, e, w in zip(names, leftover, nw):
                        val = e[:w]
                        v = "{0:<{width}} ".format(val, width=w)
                        if len(v) <= w:
                            v = v + ' ' * (w + 1 - len(v))
                        row = row + v
                        if len(e[w:]) > 0:
                            # repeat the lines with the remainders
                            newleftover.append(e[w:])
                        else:
                            newleftover.append('')
                    leftover = newleftover
                    morerows = max(len(e) for e in leftover)
                    if row:
                        answer += row + '\n'
            return answer[:-1]

        if style == 'html':
            answer += "<table> <thead>\n"
            row = "<tr>"
            for h in names:
                row = row + f"<th>{h}</th>"
            row = row + "</tr>"
            answer += row + '\n'
            answer += "</thead><tbody>\n"

            for t in tuples:
                row = "<tr>"
                for f in t:
                    if nullvalue and not f:
                        v = ''
                    else:
                        v = f
                    row = row + f"<td>{v}</td>"
                row = row + "</tr>"
                answer += row + '\n'
            answer += "</tbody></table>\n"
            return answer

        if style == 'json':
            answer = "[\n"
            comma = ''
            for t in tuples:
                row = {}
                for h, e in zip(names, t):
                    if not e and nullvalue:
                        row.update({str(h): nullvalue})
                    else:
                        row.update({str(h): e})
                answer += f"{comma}{json.dumps(row, indent=4)}\n"
                comma = ','
            answer += "]\n"
    except Exception as msg:
        print(f"general exception {msg}")
    return answer

def fetchall(qry, debug=False):
    curr = conn.cursor()
    try:
        res = curr.execute(qry)
        rows = res.fetchall()
        if debug:
            print(f"answer:{rows}")
        return rows, res.description
    except monetdbe.DatabaseError as msg:
        print('ERROR fetchone:', msg)
    return None, None


def mclient_interpreter(lines):
    global timerformat, formatter, screensize, outputfile
    for line in lines:
        if line == '\\q':
            break
        if line == '\\?':
            usage()
            continue
        if line.startswith('\\>'):
            outputfile = line[2:].strip()
            print(f"Output redirected to file '{outputfile}'")
            continue

        if line.startswith('\\<'):
            inputfile = line[2:].strip()
            try:
                inp = open(inputfile, "r")
                lines = inp.readlines()
                mclient_interpreter(lines)
            except IOError as msg:
                print(f"Could not open inputfile '{inputfile}': {msg}")
            continue

        if line.startswith('\\b'):
            bookfile = line[2:].strip()
            if bookfile:
                print(f"Book from file '{bookfile}'")
                loadbook(bookfile)
            else:
                showbook()
            continue

        if line.startswith('\\t'):
            timerformat = line[2:].strip()
            if timerformat not in ['clock', 'performance', '']:
                print(f"Timer not in ['clock', 'performance', '']")
                return
            if not timerformat:
                timerformat = None
            print(f"Timer set to '{timerformat}'")
            continue

        if line.startswith('\\f'):
            formatter = line[2:].strip()
            if formatter not in ['default', 'csv', 'tab', 'raw', 'sql', 'pandas',
                                 'line', 'keyvalue', 'html', 'json', 'rowcount', '']:
                print(f"Formatter not in [default, csv, tab, raw, sql, pandas"
                      f"line, keyvalue,  html, json, rowcount]")
                return
            if not formatter:
                formatter = None
            print(f"Format set to '{formatter}'")
            continue

        if line.startswith('\\w'):
            try:
                screensize = int(line[2:].strip())
            except ValueError as msg:
                print(f"Screen width should be an integer:{msg}")
                continue

        try:
            # first check the cookbook for a commando
            cmd = line.split(' ')[0]
            #  print('cmd', cmd)

            if cmd == 'help' or line[0] == '?':
                for e in book.keys():
                    print(f"{e} :{book[e]}")
                continue

            # we can also call a python function
            if cmd in book and cmd[0] == '.':
                rows, hdr = book[cmd]()
                print(render(hdr, rows, style=formatter))
                continue

            if cmd in book:
                line = line[len(cmd):].strip()
                #  print(f"split '{line}'")
                if line:
                    args = line.split(',')
                else:
                    args = []
                #  print(f"argument'{args}'")

                line = book[cmd]
                #  print(f"precooked '{line}'")
                # cmd v1,v2,... are filled in
                for a in args:
                    line = line.replace('@', a)

            if line.find('@') >= 0:
                print('missing arguments', line)
                continue

            #  print('action', line)
            clk = time.time()
            rows, hdr = db.fetchall(line)

            clk = time.time() - clk
            if formatter == 'rowcount':
                print(f"{len(rows)}")
            elif outputfile:
                try:
                    out = open(outputfile, "w")
                    out.write(render(hdr, rows, style=formatter))
                except IOError as msg:
                    print(f"Could not open outputfile '{outputfile}': {msg}")
            else:
                print(render(hdr, rows, style=formatter), end=None)
                #  db.print(rows)

            if line.startswith('\\|'):
                print(f"Sent result to process")

            if timerformat == 'clock':
                print(f"clk: {'%5.6f' % (clk * 1000) } ms")
        except monetdbe.DatabaseError as msg:
            print('Error:', msg)


def mclient():
    print(f"Welcome to mclient, the MonetDB/SQL interactive terminal (unreleased)")
    print("Type \\q to quit, \\? for a list of available commands")
    print("auto commit mode: on")

    # mimic mclient
    while True:
        line = input('> ')
        if not line:
            continue
        if line == '\\q':
            break
        mclient_interpreter([line])

if __name__ == "__main__":
    desc = f"Mclient simulator \n" \
           f"After database connection is established a mini mclient is started"\
           f"A collection of parameterised queries are defined in a book, but also" \
           f"freeformat queries can be formulated"
    arg_parser = argparse.ArgumentParser(description=desc)
    arg_parser.add_argument('-d', '--database', type=str, default=":memory:", help="Location of the database")

    args = arg_parser.parse_args()

    print('establish contact', args)
    conn = monetdbe.connect(args.database)
    if not conn:
        print(f"Could not access the memory {args.database}")
        exit -1
    
    mclient()
