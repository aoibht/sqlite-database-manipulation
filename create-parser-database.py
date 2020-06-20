"""
Notes are needed

This cell generates a sqlite database which can be used to validate hl7 v2 messages.

I am going to write the parser in rust because that is hype. three months later that is done.

The methods below use another sqlite database i created, essentially storing the data in a less modular form

The way this database is structured is specifically for an HL7 parser.

"""
import sqlite3

"-------------------------------------------------------Create a hl7 validation database--------------------------------------------------------------------------"
def create_database(connection, conn):
    version_list = ["2.2", "2.3", "2.3.1", "2.4", "2.5", "2.5.1", "2.6", "2.7", "2.7.1"]
    for version in version_list:
        unique_segments = find_unique_segments(version, conn)
        for segment in unique_segments:
            try:
                create_segment_table(version, segment, conn, connection)
            except Exception as darn: 
                print(darn)
                print("Didn't work for", version, segment)
                
def create_segment_table(version, segment_name, conn, connection):
    "Initialize the table"
    curs = connection.cursor()
    initialize_table(version, segment_name, curs)
    "1.) generate the nested list representation"
    "2.) generate a nested list of all leaf node"
    segment_list = create_segment(version, segment_name, 0, conn) #1
    all_nodes = find_all_elements(segment_list) #2
    
    "Now iterate through this list and insert a row for each node"
    element_count = 0
    for element in all_nodes:
        element_count += 1

        if isinstance(element, list):
            sub_element_count = 0
            for sub_element in element:
                sub_element_count += 1

                if isinstance(sub_element, list):
                    sub_component_count = 0
                    for sub_component in sub_element:
                        sub_component_count += 1

                        if isinstance(sub_component, list):
                            sub_projection_count = 0
                            for sub_projection in sub_component:
                                sub_projection_count += 1
                                #What to do with sub-projections
                                sub_projection_node = [sub_projection, element_count, 
                                                       sub_element_count, sub_component_count, sub_projection_count] 
                                create_segment_row(version, segment_name, sub_projection_node, curs)       
                        #What to do with sub-components        
                        else: 
                            sub_component_node = [sub_component, element_count,
                                                  sub_element_count, sub_component_count, 0]
                            create_segment_row(version, segment_name, sub_component_node, curs) 

                #What to do with sub-elements
                else:
                    sub_element_node = [sub_element, element_count, sub_element_count, 0, 0]
                    create_segment_row(version, segment_name, sub_element_node, curs)
                    
        #what to do with elements
        else: 
            element_node = [element, element_count, 0, 0, 0]
            create_segment_row(version, segment_name, element_node, curs)
    return True

"--------------------------------------------------------Create a relational table--------------------------------------------------------------------------"
def initialize_table(version, segment_name, curs):
    create_string = "CREATE TABLE '"  
    create_string += "data" + version.replace(".", "") + "" + segment_name
    create_string += """'(path TEXT PRIMARY KEY,
                          element INTEGER, 
                          sub_element INTEGER, 
                          sub_component INTEGER, 
                          sub_projection INTEGER
                        )"""
    curs.execute(create_string)

def create_segment_row(version, segment_name, node_list, curs):
    insert_string = "INSERT INTO '"
    insert_string += "data"+version.replace(".", "") + "" + segment_name + "' VALUES ("
    insert_string +="'" + str(node_list[0]) + "'," + str(node_list[1]) + "," +  str(node_list[2]) + ","   
    insert_string +=  str(node_list[3]) + "," +  str(node_list[4]) + ")"
    curs.execute(insert_string)
    


"--------------------------------------------------------Find Leaf Nodes--------------------------------------------------------------------------"
def find_all_elements(segment_list):
    element_list = []
    for element in segment_list: 
        if not isinstance(element[1], list):
            element_list.append(element[0])
        else:
            sub_elements = find_all_sub_elements(element)
            element_list.append(sub_elements)
    return element_list

def find_all_sub_elements(element):
    sub_element_name = element[0]
    sub_element_values = element[1]
    sub_element_list = []
    for sub_element in sub_element_values:
        if not isinstance(sub_element, list):
            node_name = sub_element_name+"-"+sub_element
            sub_element_list.append(node_name)
        else:
            sub_components = find_all_sub_components(sub_element_name, sub_element)
            sub_element_list.append(sub_components)
    return sub_element_list

def find_all_sub_components(sub_element_name, sub_element):
    sub_component_name = sub_element[0]
    sub_component_values = sub_element[1]
    sub_component_list = []
    for sub_component in sub_component_values:
        if not isinstance(sub_component, list):
            node_name = sub_element_name+"-"+sub_component_name+"-"+sub_component
            sub_component_list.append(node_name)
            temp_parent = sub_component
        else: 
            sub_projections = find_all_sub_projections(sub_element_name, sub_component_name, 
                                                       temp_parent, sub_component)
            sub_component_list.append(sub_projections)
    return sub_component_list

def find_all_sub_projections(sub_element_name, sub_component_name, temp_parent, sub_component):
    sub_projection_list = []
    for sub_projection in sub_component:
        sub_projection_list.append(sub_element_name +"-"+sub_component_name +"-"+temp_parent +"-"+ sub_projection)
    return sub_projection_list


"--------------------------------------------------------Helper Methods-------------------------------------------------------------------------"

def create_segment(version, segment_name, max_elements, connection):
    c = connection.cursor()
    segment_query = "SELECT description, data FROM 'segment_"
    segment_query += version+"_"+segment_name
    segment_query += "' ORDER BY piece"
    segment_list = []   
    for element in c.execute(segment_query):
        num_elements = 3
        name = element[0].replace(" ", "")
        name = name.replace("-", "")
        structure_type = element[1]
        try:
            element = handle_element(version, structure_type, name, True, max_elements, num_elements, connection)
            element_list = [name, element]
            segment_list.append(element_list) 
        except Exception as darn: 
            continue
            #print(darn)
    return segment_list


"_______________________________Methods Extracting Schema of the Data Structure_______________________________"
def handle_element(version, structure, name, first_flag, max_elements, num_elements, connection):
    #check to see if the user has turned off this element
    num_elements -= 1
    if num_elements < max_elements:
        if first_flag == True:return name
        else: return None
    #create a cursor for the db
    sql_element = connection.cursor()
    #construct a query to the sqlite database using the input parameters
    data_query = "SELECT piece, description, data structure, code_table  FROM 'data_"
    data_query += version+"_"+structure
    data_query += "' WHERE piece != 0 ORDER BY piece"
    element = []
    #execute the constructed query
    for data in sql_element.execute(data_query):
        description = data[1].replace(" ", "")
        description = description.replace("-", "")
        temp_structure = data[2]
        temp_code_table = data[3]
        if temp_structure == "0":
            if first_flag == True: return name
            else: return None
        else:
            element_components = handle_element(version, temp_structure, name, False, max_elements, num_elements, connection)
            if element_components == None:
                element.append(description)   
            else:
                if isinstance(element_components[1], list):
                    temp_element = []
                    for ele in element_components:
                        temp_element.append(ele[0])
                        temp_element.append(ele[1])
                    element.append([description, temp_element])
                else: 
                    element.append([description, element_components])
    return element

"This method return a list of unique segment names for a version"
def find_unique_segments(version, conn):
    message_db = conn.cursor()
    message_query = 'SELECT * FROM message_schemas WHERE version = (?) '
    #message_structure = message_structure.split("~")
    version_unique = []
    
    for data in message_db.execute(message_query, [version]):
        #print(data[2])
        segments = data[2]
        segments = segments.split("~")
        structured = ["[", "]", "{", "}", ">", "<", "|", "Zxx", "Hxx", "?"]
        for temp_segment in segments:
            if temp_segment not in structured:
                unique_flag = 1
                for already in version_unique:
                    if already == temp_segment:
                        unique_flag = 0
                if unique_flag == 1:
                    version_unique.append(temp_segment)
    return version_unique

#old database
conn = sqlite3.connect('message_structures.db')
#new database
database_directory = './segment_validator.db'

connection = sqlite3.connect(database_directory)
create_database(connection, conn)

connection.commit()
connection.close()

conn.commit()
conn.close()
    