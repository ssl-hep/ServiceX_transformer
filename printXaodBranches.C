void printXaodBranches() {
    xAOD::Init();
    TFile* f = TFile::Open("../xaodFiles/AOD.11182705._000001.pool.root.1");
    TTree* t = xAOD::MakeTransientTree(f);
    gSystem->RedirectOutput("temp.txt", "w");
    t->Show();
    gSystem->RedirectOutput(0);
}
