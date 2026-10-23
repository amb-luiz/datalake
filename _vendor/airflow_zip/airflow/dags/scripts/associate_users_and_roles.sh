#!/bin/bash

echo "Verify jq is already installed"
var=$(yum list installed jq | grep "jq")

if [ -z "$var" ]; then 
  yum install jq -y
else 
  echo "ALREADY INSTALLED $var"
fi

JSON_USERS=$(airflow users list -o json)
JSON_ROLES=$(airflow roles list -o json)

echo "USERS=$JSON_USERS"
echo "ROLES IN ENVIRONMENT=$JSON_ROLES"

if [[ $(ls | grep "users.json") ]]; then
    rm users.json
fi

if [[ $( ls| grep "roles.json") ]]; then
    rm roles.json
fi

echo $JSON_USERS > users.json
echo $JSON_ROLES > roles.json


#first check if user_no_dag_perm role is already added to the user
echo "+Adding user_no_dag_perm role to all users"
jq -c ".[]" users.json | while read j; do
  USERNAME=$(echo $j |jq -r '.username')
  USER_ROLES=$(echo $j | jq -r '.roles' | sed 's/^.//')
  ROLE="user_no_dag_perm"
  # echo $USER_ROLES
  #check if username contains the name of the role on it
  ROLE_ALREADY_ADDED=0
  for k in $(echo $USER_ROLES | sed "s/,/ /g")
  do
    #check if the role is already associate to the user, and if not, associate it 
    if [ \"$ROLE\" == $k ]; then
      echo "+$USERNAME already contains role $ROLE"
      ROLE_ALREADY_ADDED=1
    fi
  done
  #add role if not already added
  if [ $ROLE_ALREADY_ADDED == 0 ]; then
    echo "+need to add role $ROLE to user $USERNAME"
    echo "+airflow users add-role -u $USERNAME -r $ROLE"
    airflow users add-role -u $USERNAME -r $ROLE
  fi
done


#CHECK IF THE "PISTA"'S ROLE EXISTS IN THE USER, AND IF NOT, ADD IT
echo "+ Adding specific roles to each user"
jq -c ".[]" roles.json | while read i; do
  #get the existing role in the environment
  ROLE=$(echo $i |jq -r '.name' | tr '-' '_'| sed -e 's/^[[:space:]]*//' )
  jq -c ".[]" users.json | while read j; do
    #get the username
    USERNAME=$(echo $j |jq -r '.username')
    #prepare username, replacing - for _
    USERNAME_STRING_VALIDATOR=$(echo "$USERNAME" | sed -e 's/^[[:space:]]*//')
    
    USERNAME_STRING_VALIDATOR=$(awk -F "GG_DATALAKE_" '{print $2}' <<< $USERNAME_STRING_VALIDATOR)
    USERNAME_STRING_VALIDATOR=$(echo $USERNAME_STRING_VALIDATOR | tr '[:upper:]' '[:lower:]' | tr '-' '_' )
    USERNAME_STRING_VALIDATOR=$(echo ${USERNAME_STRING_VALIDATOR:0:8})
    
    #create list of roles associate to this user
    USER_ROLES=$(echo $j | jq -r '.roles' | sed 's/^.//' )
    #check if username contains the name of the role on it
    case "$USERNAME_STRING_VALIDATOR" in
    "ampli_no") USERNAME_STRING_VALIDATOR="cdl_ampli" ;;
    "ampli_su") USERNAME_STRING_VALIDATOR="cdl_ampli"  ;;
    "backbone") USERNAME_STRING_VALIDATOR="cdl_backboneeducacional"  ;;
    "crescime") USERNAME_STRING_VALIDATOR="cdl_crescimento"  ;;
    "digitalf") USERNAME_STRING_VALIDATOR="cdl_digitalfinance"   ;;
    "gente_no") USERNAME_STRING_VALIDATOR="cdl_gente"  ;;
    "gente_su") USERNAME_STRING_VALIDATOR="cdl_gente"  ;;
    "ito_nons") USERNAME_STRING_VALIDATOR="cdl_ito"  ;;
    "ito_supe") USERNAME_STRING_VALIDATOR="cdl_ito"  ;;
    "plan_cai") USERNAME_STRING_VALIDATOR="cdl_planejamentocaixa"  ;;
    "planej_c") USERNAME_STRING_VALIDATOR="cdl_planejamentocaixa"  ;;
    "platafor") USERNAME_STRING_VALIDATOR="cdl_plataforma"  ;;
    "polos_no") USERNAME_STRING_VALIDATOR="cdl_polos"  ;;
    "polos_su") USERNAME_STRING_VALIDATOR="cdl_polos"  ;;
    "relac_al") USERNAME_STRING_VALIDATOR="cdl_relacionamentoaluno"  ;;
    *)  USERNAME_STRING_VALIDATOR=$(awk -F "_" '{print $1}' <<< $USERNAME_STRING_VALIDATOR) ;; # "$USERNAME_STRING_VALIDATOR" 
    esac
    
    STR="$ROLE"
    SUB="$USERNAME_STRING_VALIDATOR"
    if [[ ! -z "$SUB" && "$STR" == *"$SUB"* ]]; then
    # if [[ "$USERNAME_STRING_VALIDATOR" == *"$ROLE"* ]]; then
    echo "#######"
      #iterate through all roles associate to the user
      ROLE_ALREADY_ADDED=0
      for k in $(echo $USER_ROLES | sed "s/,/ /g")
      do
        #check if the role is already associate to the user, and if not, associate it 
        if [ \"$ROLE\" == $k ]; then
          echo "+$USERNAME already contains role $ROLE"
          ROLE_ALREADY_ADDED=1
        fi
      done
      #add role if not already added
      if [[ $ROLE_ALREADY_ADDED == 0  &&  \"$ROLE\" != "Admin"  &&  \"$ROLE\" != "Op"  &&  \"$ROLE\" != "User"  &&  \"$ROLE\" != "Viewer" ]]; then
        echo "+need to add role $ROLE to user $USERNAME"
        echo "+airflow users add-role -u $USERNAME -r $ROLE"
        airflow users add-role -u $USERNAME -r $ROLE
      fi
    fi
  done
done

rm users.json
rm roles.json