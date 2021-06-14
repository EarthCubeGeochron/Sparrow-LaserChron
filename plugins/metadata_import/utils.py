from IPython import embed
import math

def material_check(db, material):
    '''Checks if material exists in database.
        If passed material is new, it is added to vocabulary.material before the sample is updated.
    '''
    if material is None:
        pass
    elif type(material) == float:
        # some are nan
        pass
    else:
        Material = db.model.vocabulary_material    

        current_materials = db.session.query(Material).all()

        ## This has to happen otherwise the materials has ()'s and ""'s around it
        current_material_list = []
        for row in current_materials:
            current_material_list.append(row.id)

        if material not in current_material_list: 
            mat = Material(id=material)
            db.session.add(mat)
            try:
                db.session.commit()
            except:
                db.session.rollback()
