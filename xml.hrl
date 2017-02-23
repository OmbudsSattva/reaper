%%%-------------------------------------------------------------------
%%% @author Shailen Karur <ombuds.sattva@gmail.com>
%%% @copyright (C) 2014, Shailen Karur
%%% @doc
%%%
%%% @end
%%% Created :  14 Dec 2013 by Shailen Karur <ombuds.sattva@gmail.com>
%%%-------------------------------------------------------------------
-record(xmlel,
{
    name = <<"">> :: binary(),
    attrs    = [] :: [attr()],
    children = [] :: [xmlel() | cdata()]
}).

-type(cdata() :: {xmlcdata, CData::binary()}).

-type(attr() :: {Name::binary(), Value::binary()}).

-type(xmlel() :: #xmlel{}).
