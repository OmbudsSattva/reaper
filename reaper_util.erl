%% @author Shailen Karur
%% @copyright 2014 Shailen Karur

%% @author <Shailen Karur <ombuds.sattva@gmail.com>
%% @doc This is a rewrite by Shailen Karur of original module from ProcessOne to work on eBE

-module(reaper_util).

-export([get_json_property/2, get_json_string/2,
	 format_value_content/1,
	 get_json/1, set_json/1,
	 get_packages/2, get_config/2]).

get_json_property(Key, Props) ->
    case lists:keyfind(Key, 1, Props) of
	false -> null;
	{Key, Val} -> Val
    end.

get_json_string(Key, Props) ->
    case get_json_property(Key, Props) of
	null -> <<"null">>;
	Prop when is_list(Prop) -> iolist_to_binary(Prop);
	Prop when is_integer(Prop) -> integer_to_binary(Prop);
	Prop when is_binary(Prop) -> Prop
    end.

%% We attempt to turn the content into an integer or a float.  If unsuccessful, return it as a (binary) string.
format_value_content(Int) when is_integer(Int) ->
    Int;
format_value_content(Float) when is_float(Float) ->
    Float;
format_value_content(Str) when is_list(Str) ->
    format_value_content(iolist_to_binary(Str));
format_value_content(Content) ->
    S = str:strip(Content),
    case catch binary_to_integer(S) of
	{'EXIT', _} ->
	    case catch binary_to_float(S) of
		{'EXIT', _} -> S;
		Float -> Float
	    end;
	Int ->
	    Int
    end.

get_json(Data) -> mochijson2:decode(Data).
set_json(Data) -> iolist_to_binary(mochijson2:encode(Data)).

get_packages(Props, IsRequired) ->
    case get_json_property(<<"packages">>, Props) of
	Packages when is_list(Packages) ->
	    Packages;
	_ ->
	    case IsRequired of
		true -> {error, packages_required};
		false -> undefined
	    end
    end.

get_config(Props, IsRequired) ->
    case get_json_property(<<"config_xml">>, Props) of
	Config when is_binary(Config) ->
	    xml_stream:parse_element(Config);
	_ ->
	    case IsRequired of
		true -> {error, config_required};
		false -> undefined
	    end
    end.
