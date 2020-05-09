  Prescription getPrescritionFarmacQty0(Patient patient, List<SyncTempDispense> syncTempDispense, Session sess) {
    Prescription prescription = null;
    prescription = PackageManager.getPrescriptionFromPatient(sess, patient, ((SyncTempDispense)syncTempDispense.get(0)).getDispensedate());
    if (prescription == null) {
      prescription = new Prescription();
      Session s = HibernateUtil.getNewSession();
      SimpleDateFormat df = new SimpleDateFormat("yyMMdd");
      Doctor doctorProvider = PrescriptionManager.getProvider(sess);
      LinhaT linhat = AdministrationManager.getLinha(sess, ((SyncTempDispense)syncTempDispense.get(0)).getLinhaid());
      RegimeTerapeutico regimeTerapeutico = AdministrationManager.getRegimeTerapeutico(sess, ((SyncTempDispense)syncTempDispense.get(0)).getRegimeid());
      prescription.setClinicalStage(0);
      prescription.setCurrent('T');
      prescription.setDate(((SyncTempDispense)syncTempDispense.get(0)).getDate());
      prescription.setEndDate(((SyncTempDispense)syncTempDispense.get(0)).getEnddate());
      prescription.setDoctor(doctorProvider);
      prescription.setDuration(((SyncTempDispense)syncTempDispense.get(0)).getDuration().intValue());
      prescription.setModified(((SyncTempDispense)syncTempDispense.get(0)).getModified().charValue());
      prescription.setPatient(patient);
      prescription.setPrescriptionId(patient.getPatientId() + "-" + df.format(((SyncTempDispense)syncTempDispense.get(0)).getDate()) + " - Farmac");
      prescription.setReasonForUpdate(((SyncTempDispense)syncTempDispense.get(0)).getReasonforupdate());
      prescription.setNotes("FARMAC: " + ((SyncTempDispense)syncTempDispense.get(0)).getNotes());
      prescription.setRegimeTerapeutico(regimeTerapeutico);
      prescription.setLinha(linhat);
      prescription.setDatainicionoutroservico(((SyncTempDispense)syncTempDispense.get(0)).getDatainicionoutroservico());
      prescription.setMotivoMudanca(((SyncTempDispense)syncTempDispense.get(0)).getMotivomudanca());
      prescription.setPpe(((SyncTempDispense)syncTempDispense.get(0)).getPpe().charValue());
      prescription.setPtv(((SyncTempDispense)syncTempDispense.get(0)).getPtv().charValue());
      prescription.setTb(((SyncTempDispense)syncTempDispense.get(0)).getTb().charValue());
      prescription.setGaac(((SyncTempDispense)syncTempDispense.get(0)).getGaac().charValue());
      prescription.setAf(((SyncTempDispense)syncTempDispense.get(0)).getAf().charValue());
      prescription.setFr(((SyncTempDispense)syncTempDispense.get(0)).getFr().charValue());
      prescription.setCa(((SyncTempDispense)syncTempDispense.get(0)).getCa().charValue());
      prescription.setSaaj(((SyncTempDispense)syncTempDispense.get(0)).getSaaj().charValue());
      prescription.setCcr(((SyncTempDispense)syncTempDispense.get(0)).getCcr().charValue());
      prescription.setTpc(((SyncTempDispense)syncTempDispense.get(0)).getTpc().charValue());
      prescription.setTpi(((SyncTempDispense)syncTempDispense.get(0)).getTpi().charValue());
      prescription.setDrugTypes(((SyncTempDispense)syncTempDispense.get(0)).getDrugtypes());
      List<PrescribedDrugs> prescribedDrugsList = new ArrayList<>();
      for (int i = 0; i < syncTempDispense.size(); i++) {
        Drug drug = DrugManager.getDrug(sess, ((SyncTempDispense)syncTempDispense.get(i)).getDrugname());
        PrescribedDrugs newPD = new PrescribedDrugs();
        if (drug.getPackSize() > 30) {
          newPD.setAmtPerTime(2.0D);
        } else {
          newPD.setAmtPerTime(1.0D);
        } 
        newPD.setDrug(drug);
        newPD.setModified(((SyncTempDispense)syncTempDispense.get(i)).getModified().charValue());
        newPD.setPrescription(prescription);
        newPD.setTimesPerDay(((SyncTempDispense)syncTempDispense.get(i)).getTimesperday().intValue());
        prescribedDrugsList.add(newPD);
      } 
      PackageManager.saveNewPrescription(sess, prescription, true);
    } 
    return prescription;
  }