# name: decoder.py
# author: Roy Roth (roykroth@gmail.com) 
# updated to 5.0 by Evan Argyle (evan.argyle@gmail.com)
# description: This file decodes the data we get from NASDAQ. It comes
# in a binary format. Documentation for ^Fthe formatting of the data can be 
# found at 
# http://www.nasdaqtrader.com/content/technicalsupport/specifications/dataproducts/NQTVITCHSpecification.pdf
print("Using Pure Python")
import struct as st
# Now, create a Struct instance for each of the possible message types.
# This not only makes the code more readable, but also makes decoding faster

# ---------------------------------------------------------
mess_len = st.Struct('>H')
mess_type = st.Struct('>c')  # I use this is used to determine the message type
# 4.1 Time Stamp - Seconds
time = st.Struct('>I') # T #T messages no longer sent in 5.0
# 4.2 System Message Events
system = st.Struct('>HH6sc')  # S #MODIFIED
# 4.3 Stock Related Messages
stock_dir = st.Struct('>HH6s8sccIcc2scccccIc') #R #MODDIFIED. ADDITIONAL PARAMS 
stock_trading = st.Struct('>HH6s8scc4s') #H #MODIFIED
reg_sho = st.Struct('>HH6s8sc') #Y #MODIFIED
mark_par_pos = st.Struct('>HH6s4s8sccc') #L #MODIFIED
# 4.4 Add Order Message
add_ord = st.Struct('>HH6sQcI8sI') #A #MODIFIED
add_ord_mpid = st.Struct('>HH6sQcI8sI4s') #F #MODIFIED 
# 4.5 Modify Order Messages
exec_ord = st.Struct('>HH6sQIQ') #E #MODIFIED
exec_ord_p = st.Struct('>HH6sQIQcI') #C #MODIFIED
canc_ord = st.Struct('>HH6sQI') #X #MODIFIED
del_ord = st.Struct('>HH6sQ') #D #MODIFIED
repl_ord = st.Struct('>HH6sQQII') #U #MODIFIED
# 4.6 Trade Messages
trade_nc = st.Struct('>HH6sQcI8sIQ') #P #MODIFIED
trade_cr = st.Struct('>HH6sQ8sIQc') #Q #MODIFIED
broke_tr = st.Struct('>HH6sQ') #B #MODIFIED
# 4.7 Net Order Imbalance Indicator Message
noii = st.Struct('>HH6sQQc8sIIIcc') #I #MODIFIED
# 4.8 Retail Price Improvement Indicator
rpii = st.Struct('>HH6s8sc') #N #MODIFIED
"""
NEW MESSAGES IN 5.0
"""
#BREAKER MESSAGES
decline_level=st.Struct('>HH6sQQQ') #V  [C is not a character allowed in the struct module.  I need to figure out what the problem is here.
breach=st.Struct('>HH6sc') #W
ipo_quote=st.Struct('>HH6s8sIcI') #K
collar=st.Struct('>HH6s8sIIII') #J

#USE THIS TO CHANGE TIME STAMP FROM 6-byte INT TO LONG INT
time_converter=st.Struct('>Q')

"""
END NEW MESSAGES
"""

# ------------------------------------------------------------
# Now I will write some code to actually do the decoding of this data, but, I 
# am still not sure what I want to do with it as I am decoding it.
count = 0


def decode_next(buf):
    """
    This function is currently in my first stage of figuring out how to
    handle this new data, and is thus very preliminary and incomplete. 
    However, the idea is that it will process a message and spit out a 
    tuple. I will add more pizzaz later, but here I am just making sure
    I am doing things correctly

    Python

    Parameters
    ----------
    buf: The file buffer from which we are reading our messages

    Returns
    -------
    type: The 1 character string indicating the type of message
    mess: The decoded message
    """
    len_size = 2  # Avoiding magic numbers here. This is the number of bytes of
    # the indicator of how long the message is
    try:
        bin_len = buf.read(len_size)
        length = mess_len.unpack(bin_len)[0]  # This is an integer telling
    except st.error:
        print("Something went wrong reading length in")
        print("bin_length: {}".format(bin_len))
        print("buf position: {}".format(buf._position))
        print("filename: {}".format(buf.buffer.filename))
        raise ValueError("Something went wrong reading length")
    # me how long the next message is
    # Now, go chomp off the next message
    bin_mess = buf.read(length)  # This contains the binary file representation
    # of the next message
    # t = mess_type.unpack(bin_mess[0])[0]  # t contains the type of message, so,
    t = chr(bin_mess[0])
    # now I know what to do with the message
    # This may be a little sketch because I am accessing these globally, but,
    # I think it will be fine
    action = {'T': time,
        'S': system,
        'R': stock_dir,
        'H': stock_trading,
        'Y': reg_sho,
        'L': mark_par_pos,
        'A': add_ord,
        'F': add_ord_mpid,
        'E': exec_ord,
        'C': exec_ord_p,
        'X': canc_ord,
        'D': del_ord,
        'U': repl_ord,
        'P': trade_nc,
        'Q': trade_cr,
        'B': broke_tr,
        'I': noii,
        'N': rpii,
        'V':decline_level,
        'W':breach,
        'K':ipo_quote,
        'J':collar}
    #print("type: {}".format(t)) 
    #print("Decode 113",len(bin_mess[1:]))
    #print('Decode 114Message Type: {}'.format(t))
    mess = action[t].unpack(bin_mess[1:])
    mess_list = list(mess)
    mess_list[2] = time_converter.unpack(b'\x00\x00' + mess[2])[0]
    mess = tuple(mess_list)
    return t, mess  # This will be improved in the future

